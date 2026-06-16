use bevy_ecs::{event::Event, message::Message, resource::Resource};
use serde::{Deserialize, Serialize};

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
    JoinMatchmaking,
    LeaveMatchmaking,
}

#[derive(Message, Debug, Clone, Serialize, Deserialize)]
pub enum ServerMessage {
    NewGlobalChatMessage {
        content: String,
        username: String,
        user_id: UserId,
    },
    MatchFound {
        ip: String,
        port: String,
        player_token: String,
    },
    Error {
        message: String,
    },
    Ping,
}
