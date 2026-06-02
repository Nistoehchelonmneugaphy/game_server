#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ClientConfig {
    pub auth_server_url: String,
    pub server_url: String,
    pub session_token: String,
}
