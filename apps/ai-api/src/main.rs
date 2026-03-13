mod auth;
mod billing;
mod clickhouse;
mod config;
mod db;
mod error;
mod llm;
mod models;
mod pipeline;
mod prompts;
mod state;

use std::{convert::Infallible, sync::Arc};

use anyhow::{Context, Result};
use async_stream::stream;
use auth::{AuthenticatedUser, WorkosAuth};
use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{
        IntoResponse,
        sse::{Event, KeepAlive, Sse},
    },
    routing::{get, post},
};
use billing::{StripeClient, apply_webhook};
use clickhouse::ClickHouseClient;
use config::Config;
use db::Store;
use error::AppError;
use models::{ChatRequest, MeResponse, StreamPayload};
use state::AppState;
use tokio::sync::mpsc;
use tower_http::{
    cors::{Any, CorsLayer},
    trace::TraceLayer,
};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    init_logging();

    let config = Arc::new(Config::from_env()?);
    let store = Arc::new(Store::connect(&config.database_url).await?);
    store.migrate().await?;

    let clickhouse = Arc::new(ClickHouseClient::new(
        &config.clickhouse_read_url,
        &config.clickhouse_write_url,
        config.clickhouse_view_db.clone(),
        config.clickhouse_usage_db.clone(),
    )?);
    clickhouse.ensure_ready().await?;
    clickhouse.ensure_usage_tables().await?;

    let llm = Arc::new(llm::LlmClient::new(&config)?);
    let auth = Arc::new(WorkosAuth::new(
        config.workos_issuer.clone(),
        config.workos_audience.clone(),
        config.admin_emails.clone(),
    )?);

    let port = config.port;
    let app_state = AppState {
        config,
        auth,
        store,
        clickhouse,
        llm,
    };

    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/v1/me", get(me))
        .route("/v1/conversations", get(list_conversations))
        .route("/v1/conversations/{id}", get(get_conversation))
        .route("/v1/usage", get(usage))
        .route("/v1/chat/stream", post(chat_stream))
        .route("/v1/billing/checkout", post(create_checkout))
        .route("/v1/billing/portal", post(create_portal))
        .route("/v1/webhooks/stripe", post(stripe_webhook))
        .with_state(app_state)
        .layer(
            CorsLayer::new()
                .allow_methods(Any)
                .allow_headers(Any)
                .allow_origin(Any),
        )
        .layer(TraceLayer::new_for_http());

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port))
        .await
        .context("binding AI API listener")?;
    info!(port, "ai-api listening");
    axum::serve(listener, app).await?;
    Ok(())
}

fn init_logging() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,ai_api=info")),
        )
        .init();
}

async fn healthz() -> &'static str {
    "ok"
}

async fn readyz(State(state): State<AppState>) -> Result<&'static str, AppError> {
    state.clickhouse.ensure_ready().await?;
    Ok("ok")
}

async fn me(
    State(state): State<AppState>,
    AuthenticatedUser(user): AuthenticatedUser,
) -> Result<Json<MeResponse>, AppError> {
    state.store.sync_user(&user).await?;
    let subscription_status = state.store.subscription_status(&user.org_id).await?;
    Ok(Json(MeResponse {
        user,
        subscription_status,
    }))
}

async fn list_conversations(
    State(state): State<AppState>,
    AuthenticatedUser(user): AuthenticatedUser,
) -> Result<Json<Vec<models::ConversationSummary>>, AppError> {
    state.store.sync_user(&user).await?;
    let rows = state
        .store
        .list_conversations(&user.org_id, &user.id)
        .await?;
    Ok(Json(rows))
}

async fn get_conversation(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    AuthenticatedUser(user): AuthenticatedUser,
) -> Result<Json<models::ConversationDetail>, AppError> {
    state.store.sync_user(&user).await?;
    let conversation = state
        .store
        .get_conversation(&user.org_id, &user.id, id)
        .await?
        .ok_or(AppError::NotFound)?;
    Ok(Json(conversation))
}

async fn usage(
    State(state): State<AppState>,
    AuthenticatedUser(user): AuthenticatedUser,
) -> Result<Json<models::UsageSnapshot>, AppError> {
    state.store.sync_user(&user).await?;
    Ok(Json(
        state.store.usage_snapshot(&user.org_id, &user.id).await?,
    ))
}

async fn chat_stream(
    State(state): State<AppState>,
    AuthenticatedUser(user): AuthenticatedUser,
    Json(request): Json<ChatRequest>,
) -> Result<Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>>, AppError> {
    state.store.sync_user(&user).await?;
    let (sender, mut receiver) = mpsc::channel::<StreamPayload>(32);
    let app_state = state.clone();
    tokio::spawn(async move {
        if let Err(err) = pipeline::execute_chat(app_state, user, request, sender.clone()).await {
            error!(error = %err, "chat pipeline failed");
            let _ = sender
                .send(StreamPayload::Failed {
                    message: err.to_string(),
                })
                .await;
        }
    });

    let stream = stream! {
        while let Some(payload) = receiver.recv().await {
            let json = serde_json::to_string(&payload).unwrap_or_else(|_| "{\"type\":\"failed\",\"message\":\"serialization error\"}".to_string());
            yield Ok(Event::default().event("message").data(json));
        }
    };
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

async fn create_checkout(
    State(state): State<AppState>,
    AuthenticatedUser(user): AuthenticatedUser,
) -> Result<Json<serde_json::Value>, AppError> {
    state.store.sync_user(&user).await?;
    let stripe = StripeClient::from_config(&state.config)
        .ok_or_else(|| AppError::BadRequest("stripe is not configured".to_string()))?;
    let url = stripe.create_checkout_session(&user).await?;
    Ok(Json(serde_json::json!({ "url": url })))
}

async fn create_portal(
    State(state): State<AppState>,
    AuthenticatedUser(user): AuthenticatedUser,
) -> Result<Json<serde_json::Value>, AppError> {
    state.store.sync_user(&user).await?;
    let stripe = StripeClient::from_config(&state.config)
        .ok_or_else(|| AppError::BadRequest("stripe is not configured".to_string()))?;
    let sub = state
        .store
        .latest_subscription(&user.org_id)
        .await?
        .ok_or_else(|| AppError::BadRequest("no subscription found".to_string()))?;
    let customer_id = sub
        .stripe_customer_id
        .ok_or_else(|| AppError::BadRequest("subscription has no stripe customer".to_string()))?;
    let url = stripe.create_portal_session(&customer_id).await?;
    Ok(Json(serde_json::json!({ "url": url })))
}

async fn stripe_webhook(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, AppError> {
    let stripe = StripeClient::from_config(&state.config)
        .ok_or_else(|| AppError::BadRequest("stripe is not configured".to_string()))?;
    let signature = headers
        .get("Stripe-Signature")
        .and_then(|value| value.to_str().ok())
        .ok_or(AppError::Unauthorized)?;
    let raw_body = std::str::from_utf8(&body)
        .map_err(|_| AppError::BadRequest("invalid webhook body".to_string()))?;
    let event = stripe
        .verify_webhook(raw_body, signature)
        .map_err(|_| AppError::Unauthorized)?;
    apply_webhook(&state.store, &event).await?;
    Ok((
        StatusCode::OK,
        Json(serde_json::json!({ "received": true })),
    ))
}
