use anyhow::{Result, bail};
use futures_util::{
    SinkExt,
    stream::{SplitSink, StreamExt},
};
use game_server_structs::websocket_communication::{ClientMessage, ClientMessageContent, ServerMessage, UserId};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Postgres};
use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    sync::Arc,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Mutex, broadcast, mpsc},
};
use tokio_tungstenite::{
    WebSocketStream, accept_async, accept_hdr_async,
    tungstenite::{
        self, Message, handshake::server::{Request, Response}, protocol::{CloseFrame, frame::coding::CloseCode}
    },
};
use tracing::{debug, error, info, instrument, warn};

#[derive(Clone)]
pub struct AppState {
    pub connected_clients: Arc<Mutex<HashMap<UserId, mpsc::Sender<ServerMessage>>>>,
    pub match_queue: Arc<Mutex<VecDeque<UserId>>>,
    pub db_pool: Pool<Postgres>,
}


#[instrument]
pub async fn ws(db_pool: Pool<Postgres>) -> Result<()> {
    let app_state = AppState {
        connected_clients: Arc::new(Mutex::new(HashMap::new())),
        match_queue: Arc::new(Mutex::new(VecDeque::new())),
        db_pool: db_pool.clone(),
    };
    let address = "127.0.0.1:9002";
    let listener = TcpListener::bind(&address)
        .await
        .expect(&format!("Couldn't bind to {address}"));
    info!("WebSocket server listening on {address}");
    let (shutdown_tx, _) = broadcast::channel::<()>(1);

    loop {
        tokio::select! {
            Ok((stream, _)) = listener.accept() => {
                let mut shutdown_rx = shutdown_tx.subscribe();
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


pub async fn handle_connection(
    mut shutdown_rx: broadcast::Receiver<()>,
    app_state: AppState,
    peer: SocketAddr,
    stream: TcpStream,
) -> Result<()> {
    let mut cookie = "".to_string();
    let get_cookie_and_user_id_from_headers = |req: &Request, response: Response| {
        for (ref header_name, header_value) in req.headers() {
            let header_name_str = header_name.as_str();
            match header_name_str {
                "cookie" => {
                    cookie = header_value
                        .clone()
                        .to_str()
                        .unwrap_or_default()
                        .to_string();
                }
                _ => {}
            }
        }

        Ok(response)
    };
    let mut ws_stream = accept_hdr_async(stream, get_cookie_and_user_id_from_headers).await?;
    debug!("Cookie: {cookie}");
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
    let (mut ws_write, mut ws_read) = ws_stream.split();
    let (tx, mut rx) = mpsc::channel::<ServerMessage>(32);
    let app_state = app_state.clone();
    '_lock: {
        app_state
            .connected_clients
            .lock()
            .await
            .insert(user_id.clone(), tx);
    }
    loop {
        tokio::select! {
            msg = ws_read.next() => {
                match handle_receive_client_msg(&mut ws_write, app_state.clone(), msg, user_id.clone()).await?{
                ContinueOrBreak::Continue => {
                    continue;
                },
                ContinueOrBreak::Break => {
                    break;
                 },
            }},
            Some(server_msg) = rx.recv() => handle_send_server_msg(server_msg, &mut ws_write).await?,
            _ = shutdown_rx.recv() => {
                println!("Closing connection due to server shutdown.");
                // Send a graceful close frame to the client
                let close_frame = tungstenite::protocol::Message::Close(None);
                let _ = ws_write.send(close_frame).await;
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
            return Ok(ContinueOrBreak::Break);
        }
        None => todo!(),
        _ => todo!(),
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
            {
                let clients = app_state.connected_clients.lock().await;
                for (_, tx) in clients.iter() {
                    tx.send(broadcast.clone()).await.ok();
                }
            }
        }
        ClientMessageContent::JoinMatchmaking => todo!(),
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
    sqlx::query!(
        "INSERT INTO global_chat_messages (user_id, content) VALUES ($1, $2)",
        user_id.0,
        message
    )
    .execute(db_pool)
    .await?;
    Ok(())
}

async fn get_username(db_pool: &Pool<Postgres>, user_id: UserId) -> Result<String> {
    let username = sqlx::query!("SELECT username FROM users WHERE user_id = $1", user_id.0,)
        .fetch_one(db_pool)
        .await?
        .username;
    Ok(username)
}
