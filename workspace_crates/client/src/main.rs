
#[tokio::main]
async fn main() {
    let client_config = server::config_reader::read_config::<ClientConfig>("client.env".into());
    println!("Starting server client.");
    println!(
        "Connecting to server at {} using session_token {}",
        client_config.server_url, client_config.session_token
    );
    let reqwest_client = reqwest::Client::new();

    let server_cookie = authenticate(
        &reqwest_client,
        &client_config.auth_server_url,
        &client_config.server_url,
        &client_config.session_token,
    )
    .await
    .expect("Authentication failed");
}

use client::ClientConfig;
// auth.rs
use reqwest::Client;
use shared::{FinalLoginStepRequest, FinalLoginStepResponse, ServerNonceResponse, auth_system_structs::public_api::{self, CreateJoinTokenResponse}, get_server_path};

pub async fn authenticate(
    reqwest_client: &Client,
    auth_server_url: &str,
    server_url: &str,
    session_token: &str,
) -> anyhow::Result<String> {
    // Step 1+2: get nonce from server
    let nonce: ServerNonceResponse = reqwest_client
        .post(get_server_path(server_url, shared::ServerAction::InitLogin))
        .send()
        .await?
        .json()
        .await?;

    // Step 3+4: exchange nonce + session_token with auth server for a server token
    let auth_resp: CreateJoinTokenResponse = reqwest_client
        .post(public_api::get_full_url(auth_server_url.to_string(), public_api::PublicAPIAction::CreateJoinToken))
        .json(&serde_json::json!({
            "nonce": nonce.0,
            "session_token": session_token,
        }))
        .send()
        .await?
        .json()
        .await?;

    let join_token = match auth_resp {
        CreateJoinTokenResponse::Success(create_join_token_success) => {
            create_join_token_success.join_token
        },
        CreateJoinTokenResponse::Error(create_join_token_error) => {
            println!("{create_join_token_error:#?}");
            return Err(anyhow::anyhow!("{create_join_token_error:#?}"));
        },
    };

    
let resp = reqwest_client
    .post(get_server_path(server_url, shared::ServerAction::FinalLoginStep))
    .json(&FinalLoginStepRequest { join_token })
    .send()
    .await?;

let final_login: FinalLoginStepResponse = resp.json().await?;
let cookie = final_login.cookie_token;
    Ok(cookie)
}
