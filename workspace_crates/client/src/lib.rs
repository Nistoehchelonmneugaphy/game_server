#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ClientConfig {
    pub server_ip: String,
    pub server_port: String,
    pub session_token: String,
}
