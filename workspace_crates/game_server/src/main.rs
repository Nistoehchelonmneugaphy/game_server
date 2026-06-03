use std::str::FromStr;

use game_server::{GameServerConfig, ok_or_return_error_http_resp};
use server::{
    http_stuff::{
        http_request_parsing::HttpRequestData,
        http_response::{Charset::Utf8, ContentType::ApplicationJson, HttpResponseData, ok},
    },
    server_config::ServerConfig,
};
use shared::{
    FinalLoginStepError,
    FinalLoginStepErrorKind::JoinTokenValidationError,
    FinalLoginStepRequest, FinalLoginStepResponse, FinalLoginStepSuccess, ServerAction,
    ServerNonceResponse,
    auth_system_structs::{
        self,
        public_api::{PublicAPIAction, ValidateJoinTokenResponse, get_full_url},
    },
};
use sqlx::{Pool, Postgres};

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    let game_server_config = server::config_reader::read_config::<GameServerConfig>(".env".into());
    println!("Starting the game server.");
    let db_pool = sqlx::PgPool::connect(&game_server_config.database_url)
        .await
        .expect("I expected pool construction to succeed :)");
    let server_config = ServerConfig::new(
        game_server_config.ip.clone(),
        game_server_config.port,
        game_server_config.rust_log_directory_path.clone(),
    );
    let handler = server::HandlerFn::new(move |request_data, server_config| {
        let db_pool = db_pool.clone();
        let game_server_config = game_server_config.clone();
        Box::pin(async move {
            route_handler(request_data, db_pool, server_config, game_server_config).await
        })
    });
}

async fn route_handler(
    request_data: HttpRequestData,
    db_pool: Pool<Postgres>,
    server_config: ServerConfig,
    game_server_config: GameServerConfig,
) -> HttpResponseData {
    let action: ServerAction = ok_or_return_error_http_resp!(request_data.path.clone().parse());
    match action {
        ServerAction::InitLogin => {
            let nonce = ok_or_return_error_http_resp!(sqlx::query!("INSERT INTO server_nonces (nonce, expires_at) VALUES (gen_random_uuid()::text, $1) RETURNING nonce", chrono::Utc::now().timestamp()).fetch_one(&db_pool).await).nonce;
            let resp = ServerNonceResponse(nonce);
            let bytes = ok_or_return_error_http_resp!(serde_json::to_vec(&resp));
            return respond(bytes);
        }
        ServerAction::FinalLoginStep => {
            let req: FinalLoginStepRequest =
                ok_or_return_error_http_resp!(serde_json::from_slice(&request_data.body_bytes));
            let join_token = req.join_token;
            let server_nonce = req.server_nonce;
            let reqwest_client = reqwest::Client::new();
            let validation_resp = ok_or_return_error_http_resp!(
                reqwest_client
                    .post(get_full_url(
                        game_server_config.auth_server_url,
                        PublicAPIAction::ValidateJoinToken
                    ))
                    .json(&auth_system_structs::public_api::ValidateJoinTokenRequest {
                        join_token,
                        server_nonce,
                    })
                    .send()
                    .await
            );
            let validation_resp: ValidateJoinTokenResponse =
                ok_or_return_error_http_resp!(validation_resp.json().await);
            match validation_resp {
                ValidateJoinTokenResponse::Valid(validate_join_token_success) => {
                    let user_id = validate_join_token_success.user_id;
                    let cookie_token = ok_or_return_error_http_resp!(sqlx::query!("INSERT INTO sessions (user_id, token) VALUES ($1, gen_random_uuid()::text) RETURNING token", user_id).fetch_one(&db_pool).await).token;
                    let final_resp =
                        FinalLoginStepResponse::Success(FinalLoginStepSuccess { cookie_token });
                    let bytes = ok_or_return_error_http_resp!(serde_json::to_vec(&final_resp));
                    return respond(bytes);
                }
                ValidateJoinTokenResponse::Invalid => {
                    let final_resp = FinalLoginStepResponse::Failure(FinalLoginStepError {
                        kind: JoinTokenValidationError,
                    });
                    let bytes = ok_or_return_error_http_resp!(serde_json::to_vec(&final_resp));
                    return respond(bytes);
                }
            }
        }
    }
    todo!()
}

fn respond(bytes: Vec<u8>) -> HttpResponseData {
    ok(bytes, ApplicationJson, Some(Utf8))
}
