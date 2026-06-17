pub mod websocket_communication;
pub use auth_system_structs;

use crate::websocket_communication::UserId;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct ServerNonceResponse(pub String);

#[derive(Debug, strum::EnumString, strum::Display, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum ServerAction {
    InitLogin,
    FinalLoginStep,
    SendGlobalChatMessage,
    ViewGlobalChat,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct FinalLoginStepRequest {
    pub join_token: String,
    pub server_nonce: String,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct FinalLoginStepSuccess {
    pub cookie_token: String,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub enum FinalLoginStepResponse {
    Success(FinalLoginStepSuccess),
    Failure(FinalLoginStepError),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct FinalLoginStepError {
    pub kind: FinalLoginStepErrorKind,
}
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub enum FinalLoginStepErrorKind {
    JoinTokenValidationError,
}


#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct SendGlobalChatMessageRequest {
    pub cookie: String,
    pub message: String,
}
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub enum SendGlobalChatMessageResponse {
    SendSuccess,
    SendError
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct ViewGlobalChatMessageRequest {
    pub cookie: String,
}
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct ViewGlobalChatMessageResponse {
    pub messages: Vec<ChatMessage>,
}
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct ChatMessage {
    pub content: String,
    pub username: String,
    pub user_id: UserId,
}


pub fn get_server_path(server_url: &str, action: ServerAction) -> String {
    let action_string = action.to_string();
    format!("{server_url}/{action_string}")
}
