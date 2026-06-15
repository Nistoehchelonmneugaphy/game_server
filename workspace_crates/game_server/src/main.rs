use anyhow::anyhow;
use game_server::{GameServerConfig, ok_or_return_error_http_resp, websocket::ws};
use game_server_structs::{
    FinalLoginStepError,
    FinalLoginStepErrorKind::JoinTokenValidationError,
    FinalLoginStepRequest, FinalLoginStepResponse, FinalLoginStepSuccess,
    SendGlobalChatMessageRequest, SendGlobalChatMessageResponse, ServerAction, ServerNonceResponse,
    ViewGlobalChatMessageRequest,
    auth_system_structs::{
        self,
        public_api::{PublicAPIAction, ValidateJoinTokenResponse, get_full_url},
    },
};
use server::{
    BackgroundSpawnTasks,
    http_stuff::{
        http_request_parsing::HttpRequestData,
        http_response::{Charset::Utf8, ContentType::ApplicationJson, HttpResponseData, ok},
    },
    log_tracing::init_tracing,
    server_config::ServerConfig,
};
use sqlx::{Pool, Postgres};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    let game_server_config = server::config_reader::read_config::<GameServerConfig>(".env".into());
    println!("Starting the game server.");
    let db_pool = sqlx::PgPool::connect(&game_server_config.database_url)
        .await
        .expect("I expected pool construction to succeed :)");
    let db_pool_clone_for_ws = db_pool.clone();
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
    let serv = server::run(
        server_config,
        env!("CARGO_PKG_VERSION").to_string(),
        handler,
        BackgroundSpawnTasks {
            new_tasks_to_spawn: None,
            cancellation_token: None,
        },
    );
    let ws = ws(db_pool_clone_for_ws.clone());
    tokio::try_join!(async { serv.await.map_err(anyhow::Error::from) }, async {
        ws.await.map_err(anyhow::Error::from)
    })
    .expect("Couldn't join?!");
    Ok(())
}

async fn route_handler(
    request_data: HttpRequestData,
    db_pool: Pool<Postgres>,
    server_config: ServerConfig,
    game_server_config: GameServerConfig,
) -> HttpResponseData {
    let path = request_data.path.clone();
    let action_str = ok_or_return_error_http_resp!(
        path.strip_prefix('/')
            .ok_or("Path did not start with slash")
    );
    let action: ServerAction = ok_or_return_error_http_resp!(
        action_str.parse(),
        format!("Could not parse action {}", request_data.path.clone())
    );
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
                    let username = validate_join_token_success.username;
                    ok_or_return_error_http_resp!(sqlx::query!(
                        "INSERT INTO users (user_id, username) VALUES ($1, $2) ON CONFLICT (user_id) DO UPDATE SET username = EXCLUDED.username WHERE users.username != EXCLUDED.username",
                        user_id,
                        username,
                    )
                    .execute(&db_pool)
                    .await);
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
        ServerAction::SendGlobalChatMessage => {
            send_global_chat_message(request_data, db_pool, server_config, game_server_config).await
        }
        ServerAction::ViewGlobalChat => {
            send_global_chat_message(request_data, db_pool, server_config, game_server_config).await
        }
    }
}

fn respond(bytes: Vec<u8>) -> HttpResponseData {
    ok(bytes, ApplicationJson, Some(Utf8))
}

async fn send_global_chat_message(
    request_data: HttpRequestData,
    db_pool: Pool<Postgres>,
    _server_config: ServerConfig,
    _game_server_config: GameServerConfig,
) -> HttpResponseData {
    let req: SendGlobalChatMessageRequest =
        ok_or_return_error_http_resp!(serde_json::from_slice(&request_data.body_bytes));
    let ValidCookieToken {
        cookie_token,
        user_id,
    } = ok_or_return_error_http_resp!(validate_cookie(db_pool.clone(), req.cookie).await);

    let resp = match sqlx::query!(
        "INSERT INTO global_chat_messages (user_id, content) VALUES ($1, $2)",
        user_id,
        cookie_token
    )
    .execute(&db_pool)
    .await
    {
        Ok(_) => SendGlobalChatMessageResponse::SendSuccess,
        Err(_) => SendGlobalChatMessageResponse::SendError,
    };
    let bytes = ok_or_return_error_http_resp!(serde_json::to_vec(&resp));
    return respond(bytes);
}
struct ValidCookieToken {
    cookie_token: String,
    user_id: String,
}
async fn validate_cookie(
    db_pool: Pool<Postgres>,
    cookie_token: String,
) -> Result<ValidCookieToken, String> {
    let row = sqlx::query!(
        "SELECT user_id FROM sessions WHERE token = $1",
        cookie_token
    )
    .fetch_one(&db_pool)
    .await
    .map_err(|e| e.to_string())?;
    Ok(ValidCookieToken {
        cookie_token,
        user_id: row.user_id,
    })
}

async fn view_global_chat(
    request_data: HttpRequestData,
    db_pool: Pool<Postgres>,
    _server_config: ServerConfig,
    _game_server_config: GameServerConfig,
) -> HttpResponseData {
    let req: ViewGlobalChatMessageRequest =
        ok_or_return_error_http_resp!(serde_json::from_slice(&request_data.body_bytes));
    let ValidCookieToken {
        cookie_token,
        user_id,
    } = ok_or_return_error_http_resp!(validate_cookie(db_pool.clone(), req.cookie).await);
    /*
    General todo idea: simplest: just pick last 10 chat messages. Show in ascending order.
    To improve later: pagination (chat will really be a forum thread - style thingie).
     */
    todo!()
}
