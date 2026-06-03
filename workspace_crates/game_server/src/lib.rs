#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct GameServerConfig {
    pub database_url: String,
}
