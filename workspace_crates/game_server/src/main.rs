use game_server::GameServerConfig;

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    let game_server_config = server::config_reader::read_config::<GameServerConfig>(".env".into());
    println!("Starting the game server.");
    let db_pool = sqlx::PgPool::connect(&game_server_config.database_url)
        .await
        .expect("I expected pool construction to succeed :)");
}
