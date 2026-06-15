pub mod websocket;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct GameServerConfig {
    pub database_url: String,
    pub ip: String,
    pub port: u64,
    pub ws_ip: String,
    pub ws_port: u64,
    pub rust_log_directory_path: String,
    pub auth_server_url: String,
}



#[macro_export]
macro_rules! ok_or_return_error_http_resp {
    ($expr:expr $(, $msg:expr)?) => {
        match $expr {
            Ok(v) => v,
            Err(e) => {
                println!("Err: {e:#?}");
                $(
                    println!("Msg: {:#?}", $msg);
                )?
                return server::http_stuff::http_response::ok_html("Error".to_string());
            }
        }
    };
}