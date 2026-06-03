use game_server::GameServerConfig;


#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    let game_server_config =
        server::config_reader::read_config::<GameServerConfig>(".env".into());
    println!("Starting the game server.");
}
