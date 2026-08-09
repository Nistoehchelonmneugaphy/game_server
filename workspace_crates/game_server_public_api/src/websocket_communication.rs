use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use bevy_ecs::message::Message;
use serde::{Deserialize, Serialize};
use strum::Display;


pub const COOKIE_KEY_NAME: &str = "cookie";
pub const COOKIE_PREFIX: &str = COOKIE_KEY_NAME;

pub fn pack_cookie(cookie: String) -> String {
    let encoded_cookie = URL_SAFE_NO_PAD.encode(cookie.as_bytes());
    let encoded_cookie = format!("{COOKIE_PREFIX}{encoded_cookie}");
    encoded_cookie
}
pub fn decode_cookie_token(encoded: &str) -> Option<String> {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    let bytes = URL_SAFE_NO_PAD.decode(encoded).ok()?;
    String::from_utf8(bytes).ok()
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct UserId(pub String);

#[derive(Message, Debug, Clone, Serialize, Deserialize)]
pub struct ClientMessage {
    pub cookie: String,
    pub content: ClientMessageContent,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientMessageContent {
    SendGlobalChatMessage { content: String },
    JoinMatchmaking { queue_type: QueueType },
    LeaveMatchmaking,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MatchmakingMessage {
    Join {
        user_id: UserId,
        queue_type: QueueType,
    },
    Leave {
        user_id: UserId,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueueType {
    Hunter,
    Survivor,
}

#[derive(Message, Debug, Clone, Serialize, Deserialize)]
pub enum ServerMessage {
    NewGlobalChatMessage {
        content: String,
        username: String,
        user_id: UserId,
    },
    MatchFound {
        match_url: String,
        player_token: String,
        team: MatchTeam,
    },
    Error {
        message: String,
    },
    Ping,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MatchPlayer {
    pub user_id: UserId,
    pub token: String,
    pub team: MatchTeam,
}
#[derive(Debug, Display,  Clone, Deserialize, Serialize)]
pub enum MatchTeam {
    Hunter,
    Survivor,
}