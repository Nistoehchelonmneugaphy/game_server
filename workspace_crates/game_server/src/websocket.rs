use anyhow::{Result, bail};
use futures_util::{
    SinkExt,
    stream::{SplitSink, StreamExt},
};
use game_server_public_api::{
    ChatMessage, websocket_communication::{
        COOKIE_PREFIX, ClientMessage, ClientMessageContent, MatchPlayer, MatchTeam, MatchmakingMessage, QueueType, ServerMessage, UserId, decode_cookie_token,
    },
};
use http::{HeaderValue, StatusCode};
use match_server::FromGameServer;
use sqlx::{Pool, Postgres};
use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{
        Mutex, broadcast,
        mpsc::{self, Sender},
    },
    time::interval,
};
use tokio_tungstenite::{
    WebSocketStream, accept_hdr_async,
    tungstenite::{
        self, Message,
        handshake::server::{ErrorResponse, Request, Response},
        protocol::{CloseFrame, frame::coding::CloseCode},
    },
};
use tracing::{debug, error, info, instrument, warn};

#[derive(Clone)]
pub struct AppState {
    pub connected_clients: Arc<Mutex<HashMap<UserId, mpsc::Sender<ServerMessage>>>>,
    pub matches: Arc<Mutex<Vec<MatchServer>>>,
    pub db_pool: Pool<Postgres>,
}
pub struct MatchServer {
    pub token: String,
    pub sender: tokio::sync::mpsc::Sender<FromGameServer>,
}

pub async fn communication_with_match_servers(
    db_pool: Pool<Postgres>,
    app_state: &AppState,
    ip: String,
    port: u64,
) {
    let address = format!("{ip}:{port}");
    let listener = TcpListener::bind(&address)
        .await
        .unwrap_or_else(|_| panic!("Couldn't bind to {address}"));
    info!("WebSocket match comms server listening on {address}");
    let (shutdown_tx, _) = broadcast::channel::<()>(1);
    loop {
        tokio::select! {
            Ok((stream, _)) = listener.accept() => {
                let shutdown_rx = shutdown_tx.subscribe();
                     let peer = stream
            .peer_addr()
            .expect("Connected streams should have a peer addr. Why did this why not have it?");
                    info!("Peer address: {}", peer);

                 tokio::spawn(handle_match_server_connection(shutdown_rx, app_state.clone(), peer, stream));
            },
            _ = server::shutdown_signal::shutdown_signal() => {
                info!("Shutdown signal");
                shutdown_tx.send(()).ok();
                break;
            }
        }
    }
}

#[instrument]
pub async fn ws(
    db_pool: Pool<Postgres>,
    ip: String,
    port: u64,
    match_server_url: String,
    match_executable_path: String,
) -> Result<()> {
    let app_state = AppState {
        connected_clients: Arc::new(Mutex::new(HashMap::new())),
        matches: Arc::new(Mutex::new(Vec::new())),
        db_pool: db_pool.clone(),
    };
    let address = format!("{ip}:{port}");
    let listener = TcpListener::bind(&address)
        .await
        .unwrap_or_else(|_| panic!("Couldn't bind to {address}"));
    info!("WebSocket server listening on {address}");
    let (shutdown_tx, _) = broadcast::channel::<()>(1);
    let shutdown_rx_for_matchmaker_task = shutdown_tx.subscribe();
    let (_match_tx, match_rx) = tokio::sync::mpsc::channel::<MatchmakingMessage>(100);
    tokio::spawn(matchmaker_task(
        match_executable_path.clone(),
        match_server_url.clone(),
        shutdown_rx_for_matchmaker_task,
        app_state.clone(),
        match_rx,
    ));
    loop {
        tokio::select! {
            Ok((stream, _)) = listener.accept() => {
                let shutdown_rx = shutdown_tx.subscribe();
                     let peer = stream
            .peer_addr()
            .expect("Connected streams should have a peer addr. Why did this why not have it?");
                    info!("Peer address: {}", peer);

                 tokio::spawn(handle_connection(shutdown_rx, app_state.clone(), peer, stream));
            },
            _ = server::shutdown_signal::shutdown_signal() => {
                info!("Shutdown signal");
                shutdown_tx.send(()).ok();
                break;
            }
        }
    }

    Ok(())
}

pub fn try_to_get_cookie_from_headers(req: &Request) -> Option<String> {
    let mut cookie = None;
    if let Some(value) = req.headers().get(COOKIE_PREFIX)
        && let Ok(s) = value.to_str() {
            cookie = Some(s.to_string());
            // native path — no protocol echo needed
            return cookie;
        }
    cookie
}

pub struct CookieOrTokenRetrieval {
    cookie_or_token: Option<String>,
}
impl CookieOrTokenRetrieval {
    fn handle(
        &mut self,
        req: &Request,
        mut response: Response,
    ) -> Result<Response, http::Response<Option<String>>> {
        self.cookie_or_token = try_to_get_cookie_from_headers(req);
        if self.cookie_or_token.is_some() {
            return Ok(response);
        }
        let (retrieved_cookie, hv) = retrieve_cookie_from_sec_websocket_protocol(req)?;
        self.cookie_or_token = retrieved_cookie;
        response.headers_mut().insert("sec-websocket-protocol", hv);
        Ok(response)
    }
}
pub async fn handle_match_server_connection(
    mut shutdown_rx: broadcast::Receiver<()>,
    state: AppState,
    peer: SocketAddr,
    stream: TcpStream,
) -> Result<()> {
    let mut token = CookieOrTokenRetrieval {
        cookie_or_token: None,
    };
    let get_token_from_headers = |req: &Request, response: Response| token.handle(req, response);
    let mut ws_stream = accept_hdr_async(stream, get_token_from_headers).await?;
    let token = match token.cookie_or_token.take() {
        Some(c) => c,
        None => bail!("No token"),
    };
    debug!("Token: {token:#?}");
    let to_match_sender = validate_token(state.clone(), token).await?;

    Ok(())
}
pub async fn validate_token(
    app_state: AppState,
    token: String,
) -> Result<tokio::sync::mpsc::Sender<match_server::FromGameServer>> {
    let matches = app_state.matches.lock().await;
    if let Some(m) = matches.iter().find(|m| m.token == "haha321") {
        return Ok(m.sender.clone());
    }
    bail!("Token validation error");
}
pub async fn handle_connection(
    mut shutdown_rx: broadcast::Receiver<()>,
    app_state: AppState,
    peer: SocketAddr,
    stream: TcpStream,
) -> Result<()> {
    let mut cookie = CookieOrTokenRetrieval {
        cookie_or_token: None,
    };
    let get_cookie_from_headers = |req: &Request, response: Response| cookie.handle(req, response);
    let mut ws_stream = accept_hdr_async(stream, get_cookie_from_headers).await?;
    let cookie = match cookie.cookie_or_token.take() {
        Some(c) => c,
        None => bail!("No cookie"),
    };
    debug!("Cookie: {cookie:#?}");

    let user_id = match validate_cookie(&app_state.db_pool, cookie.clone()).await {
        Ok(Some(id)) => id,
        Ok(None) => {
            ws_stream
                .close(Some(CloseFrame {
                    code: CloseCode::Policy,
                    reason: "Invalid cookie".into(),
                }))
                .await
                .ok();
            bail!("Invalid cookie, closing ws_stream");
        }
        Err(e) => {
            ws_stream
                .close(Some(CloseFrame {
                    code: CloseCode::Error,
                    reason: "Internal error".into(),
                }))
                .await
                .ok();
            bail!("Could not contact DB e: {e:#?}, closing ws_stream");
        }
    };

    info!("New WebSocket connection (peer, user_id): ({peer:#?}, {user_id:#?})");
    let (mut ws_writer, mut ws_reader) = ws_stream.split();
    let (tx, mut rx) = mpsc::channel::<ServerMessage>(32);

    let app_state = app_state.clone();
    match send_latest_chat_messages(&app_state.db_pool, &mut ws_writer).await {
        Ok(_) => {}
        Err(e) => {
            error!("send_latest_chat_messages error: {e:#?}");
        }
    };
    '_lock: {
        app_state
            .connected_clients
            .lock()
            .await
            .insert(user_id.clone(), tx);
    }

    let mut ping_interval = interval(Duration::from_secs(40));

    loop {
        tokio::select! {
            _ = ping_interval.tick() => {
                debug!("WS Connection ping");
                if let Err(e) = handle_send_server_msg(ServerMessage::Ping, &mut ws_writer).await {
                    println!("Failed to send ping: {}", e);
                    break;
                }
            }
            msg = ws_reader.next() => {
                match handle_receive_client_msg(&mut ws_writer, app_state.clone(), msg, user_id.clone()).await?{
                ContinueOrBreak::Continue => {
                    continue;
                },
                ContinueOrBreak::Break => {
                    break;
                 },
            }},
            Some(server_msg) = rx.recv() => {
                match handle_send_server_msg(server_msg, &mut ws_writer).await {
                    Ok(_) => {},
                    Err(e) => {
                        error!("handle_send_server_msg error: {e:#?}");
                    }
                }
            },
            _ = shutdown_rx.recv() => {
                println!("Closing connection due to server shutdown.");
                // Send a graceful close frame to the client
                let close_frame = tungstenite::protocol::Message::Close(None);
                let _ = ws_writer.send(close_frame).await;
                break;
            }
        }
    }

    Ok(())
}

async fn handle_send_server_msg(
    server_message: ServerMessage,
    ws_write: &mut SplitSink<WebSocketStream<TcpStream>, Message>,
) -> Result<()> {
    let text = serde_json::to_string(&server_message)?;
    ws_write.send(Message::Text(text.into())).await?;
    Ok(())
}
pub enum ContinueOrBreak {
    Continue,
    Break,
}

async fn handle_receive_client_msg(
    ws_write: &mut SplitSink<WebSocketStream<TcpStream>, Message>,
    app_state: AppState,
    msg: Option<Result<Message, tokio_tungstenite::tungstenite::error::Error>>,
    user_id: UserId,
) -> Result<ContinueOrBreak> {
    match msg {
        Some(Ok(Message::Text(text))) => {
            let parsed: ClientMessage = match serde_json::from_str(&text) {
                Ok(m) => m,
                Err(_) => {
                    let err = ServerMessage::Error {
                        message: "Bad message format".into(),
                    };
                    let text = serde_json::to_string(&err)?.into();
                    ws_write.send(Message::Text(text)).await.ok();
                    return Ok(ContinueOrBreak::Continue);
                }
            };
            handle_client_message(parsed, &app_state, ws_write)
                .await
                .ok();
        }
        Some(Err(e)) => {
            warn!("WebSocket error for user_id {user_id:#?}: {e:#?}");
            remove_from_connected(app_state.connected_clients.clone(), user_id).await;
            return Ok(ContinueOrBreak::Break);
        }
        None => {
            info!("Client stream ended for user {user_id:#?}");
            remove_from_connected(app_state.connected_clients.clone(), user_id).await;
            return Ok(ContinueOrBreak::Break);
        }
        _anything_else => {
            debug!("{_anything_else:#?}");
            remove_from_connected(app_state.connected_clients.clone(), user_id).await;
            return Ok(ContinueOrBreak::Continue);
        }
    }
    Ok(ContinueOrBreak::Continue)
}

async fn handle_client_message(
    msg: ClientMessage,
    app_state: &AppState,
    _ws_write: &mut SplitSink<WebSocketStream<TcpStream>, Message>,
) -> Result<()> {
    let db_pool = app_state.db_pool.clone();
    let user_id = validate_cookie(&db_pool, msg.cookie.clone())
        .await?
        .ok_or_else(|| anyhow::anyhow!("Invalid cookie token"))?;
    match msg.content {
        ClientMessageContent::SendGlobalChatMessage { content } => {
            save_chat_message(&db_pool, user_id.clone(), content.clone()).await?;
            let username = get_username(&db_pool, user_id.clone()).await?;
            let broadcast = ServerMessage::NewGlobalChatMessage {
                content: content.clone(),
                username: username.clone(),
                user_id: user_id.clone(),
            };
            debug!("Sent message {content}");
            broadcast_to_all(broadcast.clone(), app_state.connected_clients.clone()).await;
        }
        ClientMessageContent::JoinMatchmaking { queue_type: _ } => todo!(),
        ClientMessageContent::LeaveMatchmaking => todo!(),
    }
    Ok(())
}

async fn validate_cookie(db_pool: &Pool<Postgres>, cookie: String) -> Result<Option<UserId>> {
    let resp = sqlx::query!("SELECT user_id FROM sessions WHERE token = $1", cookie)
        .fetch_optional(db_pool)
        .await?;
    match resp {
        Some(r) => Ok(Some(UserId(r.user_id))),
        None => Ok(None),
    }
}

async fn save_chat_message(
    db_pool: &Pool<Postgres>,
    user_id: UserId,
    message: String,
) -> Result<()> {
    if let Err(e) = sqlx::query!(
        "INSERT INTO global_chat_messages (message_id, user_id, content) VALUES (gen_random_uuid()::text, $1, $2)",
        user_id.0,
        message
    )
    .execute(db_pool)
    .await {
        error!("{e:#?}");
        bail!(e);
    };
    Ok(())
}

async fn get_username(db_pool: &Pool<Postgres>, user_id: UserId) -> Result<String> {
    let username = sqlx::query!("SELECT username FROM users WHERE user_id = $1", user_id.0,)
        .fetch_one(db_pool)
        .await?
        .username;
    Ok(username)
}

async fn send_latest_chat_messages(
    db_pool: &Pool<Postgres>,
    ws_writer: &mut SplitSink<WebSocketStream<TcpStream>, Message>,
) -> Result<()> {
    let latest_chat_messages = get_latest_chat_messages(db_pool, 5).await?;
    for msg in latest_chat_messages {
        let server_message = ServerMessage::NewGlobalChatMessage {
            content: msg.content,
            username: msg.username,
            user_id: msg.user_id,
        };
        handle_send_server_msg(server_message, ws_writer).await.ok();
    }
    Ok(())
}
async fn get_latest_chat_messages(
    db_pool: &Pool<Postgres>,
    amount: usize,
) -> Result<Vec<ChatMessage>> {
    let amount = i64::try_from(amount)?;
    let messages = sqlx::query!(
        r#"SELECT 
    sub.user_id, 
    u.username, 
    sub.content, 
    sub.sent_at 
FROM (
    SELECT user_id, content, sent_at
    FROM global_chat_messages 
    ORDER BY primary_key DESC 
    LIMIT $1
) sub
INNER JOIN users u ON sub.user_id = u.user_id
ORDER BY sub.sent_at ASC"#,
        amount
    )
    .fetch_all(db_pool)
    .await?;
    let mut msgs = Vec::new();
    for msg in messages {
        msgs.push(ChatMessage {
            content: msg.content,
            username: msg.username,
            user_id: UserId(msg.user_id),
        });
    }
    Ok(msgs)
}

async fn broadcast_to_all(
    broadcast: ServerMessage,
    connected_clients: Arc<Mutex<HashMap<UserId, Sender<ServerMessage>>>>,
) {
    {
        let clients: Vec<(UserId, Sender<ServerMessage>)> = {
            let lock = connected_clients.lock().await;
            lock.iter()
                .map(|(id, tx)| (id.clone(), tx.clone()))
                .collect()
        };
        debug!(
            "Broadcasting a message to connected clients, count {}",
            clients.len()
        );

        let mut count = 0;
        for (user_id, tx) in clients {
            count += 1;
            debug!("Broadcast #{count}");
            if let Err(e) = tx.try_send(broadcast.clone()) {
                error!("Broadcast #{count} error: {e:#?}");
                {
                    remove_from_connected(connected_clients.clone(), user_id.clone()).await;
                    debug!(
                        "Due to the broadcast failing for this user_id ({user_id:#?}), removed the user from connected list"
                    );
                }
            };
        }
    }
}

async fn remove_from_connected(
    connected_clients: Arc<Mutex<HashMap<UserId, Sender<ServerMessage>>>>,
    user_id: UserId,
) {
    debug!("Remove {user_id:#?} from connected");
    let mut lock = connected_clients.lock().await;
    lock.remove(&user_id);
}

async fn matchmaker_task(
    executable_path: String,
    match_server_url: String,
    mut shutdown_rx: broadcast::Receiver<()>,
    app_state: AppState,
    mut match_rx: tokio::sync::mpsc::Receiver<MatchmakingMessage>,
) {
    let mut survivor_queue: VecDeque<UserId> = VecDeque::new();
    let mut hunter_queue: VecDeque<UserId> = VecDeque::new();
    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                println!("Matchmaker shutting down");
                break;
            },
            msg = match_rx.recv() => {
                matchmaking_inner(executable_path.clone(), match_server_url.clone(), app_state.clone(),&mut hunter_queue, &mut survivor_queue, msg, ).await;
            }
        }
    }
}
async fn matchmaking_inner(
    executable_path: String,
    match_server_url: String,
    app_state: AppState,
    hunter_queue: &mut VecDeque<UserId>,
    survivor_queue: &mut VecDeque<UserId>,
    msg: Option<MatchmakingMessage>,
) {
    let Some(msg) = msg else {
        return;
    };
    match msg {
        MatchmakingMessage::Join {
            user_id,
            queue_type,
        } => match queue_type {
            QueueType::Hunter => {
                hunter_queue.push_back(user_id.clone());
            }
            QueueType::Survivor => {
                survivor_queue.push_back(user_id.clone());
            }
        },
        MatchmakingMessage::Leave { user_id } => {
            // Remove the user from  the queue. How: Keeping the rest, discard the entry with the specified  leaver's user id.
            hunter_queue.retain(|queue_user_id| queue_user_id.0 != user_id.0);
            survivor_queue.retain(|queue_user_id| queue_user_id.0 != user_id.0);
        }
    }

    const SURVIVOR_PLAYERS_PER_MATCH: usize = 1;
    const HUNTER_PLAYERS_PER_MATCH: usize = 1;
    while hunter_queue.len() >= HUNTER_PLAYERS_PER_MATCH
        && survivor_queue.len() >= SURVIVOR_PLAYERS_PER_MATCH
    {
        let Some(player_1) = hunter_queue.remove(0) else {
            debug!("Hunter queue none");
            return;
        };
        let Some(player_2) = survivor_queue.remove(0) else {
            debug!("Survivor queue none");
            return;
        };
        // So, what I am going to do:
        // 1. generate a random string token for each player (used in the match on each input from player to ensure the user is authorized)
        // 2. Start match process with specified tokens, and retrieve ip and port
        // 3. Send ip and port and the token to each user
        // 4. Users connect and play
        const TOKEN_LENGTH: usize = 24;
        let token_1 = random_alphanumeric_string(TOKEN_LENGTH);
        let token_2 = random_alphanumeric_string(TOKEN_LENGTH);

        let team_1 = MatchTeam::Hunter;
        let team_2 = MatchTeam::Survivor;

        let Ok(process) = spawn_match_process(
            executable_path.clone(),
            vec![
                MatchPlayer {
                    user_id: player_1.clone(),
                    token: token_1.clone(),
                    team: team_1.clone(),
                },
                MatchPlayer {
                    user_id: player_2.clone(),
                    token: token_2.clone(),
                    team: team_2.clone(),
                },
            ],
        )
        .await
        else {
            error!("Could not start process");
            return;
        };
        let port = process.port;
        let (user_1_sender, user_2_sender) = {
            let user_1_sender = {
                let map = app_state.connected_clients.lock().await;
                map.get(&player_1).cloned()
            };
            let user_2_sender = {
                let map = app_state.connected_clients.lock().await;
                map.get(&player_2).cloned()
            };

            if let Some(user_1_sender) = user_1_sender
                && let Some(user_2_sender) = user_2_sender
            {
                (user_1_sender, user_2_sender)
            } else {
                debug!("Some user_ senders is None");
                return;
            }
        };
        let match_url = format!("{match_server_url}/{}", port);
        match user_1_sender
            .send(ServerMessage::MatchFound {
                match_url: match_url.clone(),
                player_token: token_1,
                team: team_1.clone(),
            })
            .await
        {
            Ok(_) => debug!("user_1_sender sent"),
            Err(e) => error!("user_1_sender error: {e:#?}"),
        }
        match user_2_sender
            .send(ServerMessage::MatchFound {
                match_url: match_url.clone(),
                player_token: token_2,
                team: team_2.clone(),
            })
            .await
        {
            Ok(_) => debug!("user_2_sender sent"),
            Err(e) => error!("user_2_sender error: {e:#?}"),
        }
    }
}

pub struct MatchProcess {
    pub port: u64,
    pub child_process: tokio::process::Child,
}

async fn spawn_match_process(
    executable_path: String,
    player: Vec<MatchPlayer>,
) -> Result<MatchProcess, anyhow::Error> {
    /*
    Start match server
     */
    
    todo!();

}

fn random_alphanumeric_string(length: usize) -> String {
    use rand::{RngExt, distr::Alphanumeric};
    let mut rng = rand::rng();
    let chars: String = (0..=length)
        .map(|_| rng.sample(Alphanumeric) as char)
        .collect();
    chars
}

fn retrieve_cookie_from_sec_websocket_protocol(
    req: &tungstenite::handshake::server::Request,
) -> core::result::Result<(Option<String>, HeaderValue), http::Response<Option<String>>> {
    let raw_protocols = req
        .headers()
        .get_all("sec-websocket-protocol")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|s| s.split(','))
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>();

    let cookie_prefix = COOKIE_PREFIX;
    let matched = raw_protocols.iter().find_map(|p| {
        p.strip_prefix(cookie_prefix)
            .map(|enc| (p.clone(), enc.to_string()))
    });

    let Some((full_protocol, encoded)) = matched else {
        let mut resp = ErrorResponse::new(Some("missing cookie protocol".into()));
        *resp.status_mut() = StatusCode::UNAUTHORIZED;
        return Err(resp);
    };
    let Some(decoded) = decode_cookie_token(&encoded) else {
        let mut resp = ErrorResponse::new(Some("bad cookie encoding".into()));
        *resp.status_mut() = StatusCode::UNAUTHORIZED;
        return Err(resp);
    };

    let Ok(hv) = HeaderValue::from_str(&full_protocol) else {
        let mut resp = ErrorResponse::new(Some("Could not create error resp".into()));
        *resp.status_mut() = StatusCode::UNAUTHORIZED;
        return Err(resp);
    };
    Ok((Some(decoded), hv))
}
