use futures_util::stream::StreamExt;
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Postgres};
use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr, sync::Arc,
};
use tokio::{net::{TcpListener, TcpStream}, sync::{Mutex, mpsc}};
use tokio_tungstenite::accept_async;
use tracing::info;

#[derive(Clone)]
pub struct AppState {
    pub connected_clients: Arc<Mutex<HashMap<UserId, mpsc::Sender<ServerMessage>>>>,
    pub match_queue: Arc<Mutex<VecDeque<UserId>>>,
    pub db: Pool<Postgres>,
}

#[derive(Debug, Clone, Serialize, Deserialize,PartialEq,  Eq, Hash)]
pub struct UserId(pub String);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientMessage {
    pub cookie: String,
    pub message_content: ClientMessageContent,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientMessageContent {
    SendGlobalChatMessage { content: String },
    JoinMatchmaking,
    LeaveMatchmaking,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ServerMessage {
    NewGlobalChatMessage {
        content: String,
        username: String,
        user_id: String,
    },
    MatchFound {
        ip: String,
        port: String,
        player_token: String,
    },
}

pub async fn ws(db_pool: Pool<Postgres>) {
    let app_state = AppState {
        connected_clients: Arc::new(Mutex::new(HashMap::new())),
        match_queue: Arc::new(Mutex::new(VecDeque::new())),
        db: db_pool.clone(),
    };
    let address = "127.0.0.1:9002";
    let listener = TcpListener::bind(&address)
        .await
        .expect(&format!("Couldn't bind to {address}"));
    info!("Listening on {address}");
    while let Ok((stream, _)) = listener.accept().await {
        let peer = stream
            .peer_addr()
            .expect("Connected streams should have a peer addr. Why did this why not have it?");
        info!("Peer address: {}", peer);

        tokio::spawn(accept_connection(app_state.clone(), peer, stream));
    }
}

pub async fn accept_connection(app_state: AppState, peer: SocketAddr, stream: TcpStream) {
    handle_connection(app_state, peer, stream).await;
}

pub async fn handle_connection(app_state: AppState, peer: SocketAddr, stream: TcpStream) {
    let mut ws_stream = accept_async(stream).await.expect("Failed to accept");
    info!("New WebSocket connection: {peer:#?}");
    let (mut ws_write, mut ws_read) = ws_stream.split();
    let (tx, mut rx) = mpsc::channel::<ServerMessage>(32);
    loop {
        tokio::select! {
            msg = ws_read.next() => {

            }
            Some(server_msg) = rx.recv() => {
                
            }
        }
    }
}
