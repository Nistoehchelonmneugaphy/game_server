pub use auth_system_structs;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct ServerNonceResponse(pub String);

#[derive(Debug, strum::EnumString, strum::Display, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum ServerAction {
    InitLogin,
    FinalLoginStep,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct FinalLoginStepRequest {
    pub join_token: String,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct FinalLoginStepResponse {
    pub cookie_token: String,
}
pub fn get_server_path(server_url: &str, action: ServerAction) -> String {
    let action_string = action.to_string();
    let ret = format!("{server_url}/{action_string}");
    ret
}
