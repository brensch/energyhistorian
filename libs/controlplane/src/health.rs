use std::net::SocketAddr;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use anyhow::{Context, Result};
use axum::{Json, Router, routing::get};
use serde::Serialize;
use tokio::task::JoinHandle;

use crate::ServiceRole;

#[derive(Debug, Serialize)]
struct HealthPayload<'a> {
    status: &'static str,
    service: &'a str,
}

#[derive(Debug, Clone, Default)]
pub struct HealthState {
    ready: Arc<AtomicBool>,
    startup_complete: Arc<AtomicBool>,
}

impl HealthState {
    pub fn mark_ready(&self) {
        self.ready.store(true, Ordering::SeqCst);
    }

    pub fn mark_startup_complete(&self) {
        self.startup_complete.store(true, Ordering::SeqCst);
    }

    fn is_ready(&self) -> bool {
        self.ready.load(Ordering::SeqCst)
    }

    fn is_startup_complete(&self) -> bool {
        self.startup_complete.load(Ordering::SeqCst)
    }
}

pub fn spawn_health_server(
    role: ServiceRole,
    listen_addr: SocketAddr,
    state: HealthState,
) -> JoinHandle<Result<()>> {
    tokio::spawn(async move { serve_health_server(role, listen_addr, state).await })
}

async fn serve_health_server(
    role: ServiceRole,
    listen_addr: SocketAddr,
    state: HealthState,
) -> Result<()> {
    let app = Router::new()
        .route(
            "/healthz",
            get(move || async move {
                Json(HealthPayload {
                    status: "ok",
                    service: role.as_str(),
                })
            }),
        )
        .route("/livez", get(|| async { "ok" }))
        .route(
            "/readyz",
            get({
                let state = state.clone();
                move || async move {
                    if state.is_ready() {
                        Ok("ok")
                    } else {
                        Err(axum::http::StatusCode::SERVICE_UNAVAILABLE)
                    }
                }
            }),
        )
        .route(
            "/startupz",
            get(move || async move {
                if state.is_startup_complete() {
                    Ok("ok")
                } else {
                    Err(axum::http::StatusCode::SERVICE_UNAVAILABLE)
                }
            }),
        );

    let listener = tokio::net::TcpListener::bind(listen_addr)
        .await
        .with_context(|| format!("binding health listener at {listen_addr}"))?;
    axum::serve(listener, app)
        .await
        .context("serving health endpoint")?;
    Ok(())
}
