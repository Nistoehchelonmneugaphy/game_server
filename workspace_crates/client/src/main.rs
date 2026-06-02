use client::ClientConfig;

fn main() {
    let client_config = server::config_reader::read_config::<ClientConfig>("client.env".into());
    println!("Starting server client.");
    println!("Connecting to server at {}:{} using session_token {}", client_config.server_ip, client_config.server_port, client_config.session_token);
}
