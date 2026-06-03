#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct GameServerConfig {
    pub database_url: String,
    pub ip: String,
    pub port: u64,
    pub rust_log_directory_path: String,
}
