use std::sync::atomic::Ordering;
use std::time::Instant;

use anyhow::{Context, Result};
use axum::{Json, Router, extract::State, routing::get};
use serde::Serialize;
use tokio::task::JoinHandle;

use crate::clickhouse::ClickHousePublisher;
use crate::db::Db;
use crate::orchestrator::Stats;

#[derive(Clone)]
pub struct HealthContext {
    pub db: Db,
    pub publisher: ClickHousePublisher,
    pub stats: Stats,
    pub started_at: Instant,
}

#[derive(Serialize)]
struct StatusResponse {
    status: &'static str,
    uptime_seconds: u64,
    clickhouse_ok: bool,
    stages: StageStats,
    artifacts: ArtifactCounts,
}

#[derive(Serialize)]
struct StageStats {
    discovers_ok: u64,
    discovers_err: u64,
    downloads_ok: u64,
    downloads_err: u64,
    parses_ok: u64,
    parses_err: u64,
}

#[derive(Serialize)]
struct ArtifactCounts {
    discovered: i64,
    downloaded: i64,
    parsed: i64,
    download_failed: i64,
    parse_failed: i64,
}

pub fn spawn_health_server(
    listen_addr: std::net::SocketAddr,
    ctx: HealthContext,
) -> JoinHandle<Result<()>> {
    tokio::spawn(async move {
        let app = Router::new()
            .route("/health", get(health_handler))
            .route("/status", get(status_handler))
            .with_state(ctx);

        let listener = tokio::net::TcpListener::bind(listen_addr)
            .await
            .with_context(|| format!("binding health listener at {listen_addr}"))?;
        axum::serve(listener, app)
            .await
            .context("serving health endpoint")?;
        Ok(())
    })
}

async fn health_handler() -> &'static str {
    "ok"
}

async fn status_handler(State(ctx): State<HealthContext>) -> Json<StatusResponse> {
    let clickhouse_ok = ctx.publisher.ensure_ready().await.is_ok();
    let uptime = ctx.started_at.elapsed().as_secs();

    let artifacts = {
        let conn = ctx.db.lock().await;
        let count = |status: &str| -> i64 {
            conn.query_row(
                "SELECT COUNT(*) FROM artifacts WHERE status = ?1",
                rusqlite::params![status],
                |row| row.get(0),
            )
            .unwrap_or(0)
        };
        ArtifactCounts {
            discovered: count("discovered"),
            downloaded: count("downloaded"),
            parsed: count("parsed"),
            download_failed: count("download_failed"),
            parse_failed: count("parse_failed"),
        }
    };

    Json(StatusResponse {
        status: "ok",
        uptime_seconds: uptime,
        clickhouse_ok,
        stages: StageStats {
            discovers_ok: ctx.stats.discovers_ok.load(Ordering::Relaxed),
            discovers_err: ctx.stats.discovers_err.load(Ordering::Relaxed),
            downloads_ok: ctx.stats.downloads_ok.load(Ordering::Relaxed),
            downloads_err: ctx.stats.downloads_err.load(Ordering::Relaxed),
            parses_ok: ctx.stats.parses_ok.load(Ordering::Relaxed),
            parses_err: ctx.stats.parses_err.load(Ordering::Relaxed),
        },
        artifacts,
    })
}
