mod pipeline;
mod scheduler;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use axum::extract::State;
use axum::routing::{get, post};
use axum::{Json, Router};
use clap::Parser;
use ingest_core::{
    SourceCollection, SourceDescriptor, SourceMetadataDocument, SourcePlugin, TaskKind,
};
use rusqlite::Connection;
use serde::Serialize;
use source_aemo_dvd::AemoDvdPlugin;
use source_aemo_metadata::AemoMetadataPlugin;
use source_nemweb::NemwebPlugin;

#[derive(Parser, Debug)]
#[command(author, version, about = "energyhistorian daemon service")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:8080")]
    listen_addr: SocketAddr,
    #[arg(long, default_value = "data/control-plane.sqlite")]
    state_db: PathBuf,
}

#[derive(Clone)]
struct AppState {
    db: Arc<Mutex<Connection>>,
    source_plans: Arc<Vec<SourcePlan>>,
}

#[derive(Debug, Serialize, Clone)]
struct SourcePlan {
    descriptor: SourceDescriptor,
    collections: Vec<SourceCollection>,
    metadata_documents: Vec<SourceMetadataDocument>,
}

#[derive(Debug, Serialize)]
struct EnqueueResponse {
    accepted: bool,
    task_id: String,
}

#[derive(Debug, Serialize)]
struct TaskRecord {
    task_id: String,
    source_id: String,
    collection_id: String,
    task_kind: String,
    idempotency_key: String,
    status: String,
    attempts: i64,
    available_at: String,
    lease_owner: Option<String>,
    lease_expires_at: Option<String>,
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
    source_count: usize,
    sqlite_path: String,
}

#[derive(Debug, Serialize)]
struct ControlPlaneSummary {
    tables: Vec<&'static str>,
    notes: Vec<&'static str>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let db = open_state_db(&args.state_db)?;
    let source_plans = build_source_plans();

    let state = AppState {
        db: Arc::new(Mutex::new(db)),
        source_plans: Arc::new(source_plans),
    };

    // Derive data directories from the state DB path
    let data_dir = args
        .state_db
        .parent()
        .unwrap_or_else(|| Path::new("data"))
        .to_path_buf();

    // Spawn the background scheduler/worker pool
    let scheduler_db = state.db.clone();
    tokio::spawn(async move {
        scheduler::run(
            scheduler_db,
            data_dir.join("raw"),
            data_dir.join("parsed"),
            data_dir.join("schema_registry"),
        )
        .await;
    });

    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/sources", get(list_sources))
        .route("/control-plane", get(control_plane))
        .route("/tasks", get(list_tasks))
        .route("/tasks/enqueue", post(enqueue_demo_tasks))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(args.listen_addr)
        .await
        .with_context(|| format!("binding {}", args.listen_addr))?;
    eprintln!("listening on {}", args.listen_addr);
    axum::serve(listener, app)
        .await
        .context("running HTTP service")?;
    Ok(())
}

fn build_source_plans() -> Vec<SourcePlan> {
    let plugins: Vec<Box<dyn SourcePlugin>> = vec![
        Box::new(NemwebPlugin::new()),
        Box::new(AemoMetadataPlugin::new()),
        Box::new(AemoDvdPlugin::new()),
    ];

    plugins
        .into_iter()
        .map(|plugin| SourcePlan {
            descriptor: plugin.descriptor(),
            collections: plugin.collections(),
            metadata_documents: plugin.metadata_catalog(),
        })
        .collect()
}

fn open_state_db(path: &PathBuf) -> Result<Connection> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating state db parent {}", parent.display()))?;
    }

    let conn = Connection::open(path)
        .with_context(|| format!("opening sqlite state db {}", path.display()))?;
    conn.pragma_update(None, "journal_mode", "WAL")
        .context("enabling sqlite WAL mode")?;
    conn.pragma_update(None, "synchronous", "NORMAL")
        .context("setting sqlite synchronous mode")?;
    conn.busy_timeout(std::time::Duration::from_secs(5))
        .context("configuring sqlite busy timeout")?;

    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS source_collections (
            source_id TEXT NOT NULL,
            collection_id TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (source_id, collection_id)
        );

        CREATE TABLE IF NOT EXISTS discovered_artifacts (
            artifact_id TEXT PRIMARY KEY,
            source_id TEXT NOT NULL,
            collection_id TEXT NOT NULL,
            remote_uri TEXT NOT NULL,
            discovered_at TEXT NOT NULL,
            published_at TEXT,
            status TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS fetched_artifacts (
            artifact_id TEXT PRIMARY KEY,
            local_path TEXT NOT NULL,
            content_sha256 TEXT,
            content_length_bytes INTEGER,
            fetched_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS tasks (
            task_id TEXT PRIMARY KEY,
            source_id TEXT NOT NULL,
            collection_id TEXT NOT NULL,
            task_kind TEXT NOT NULL,
            idempotency_key TEXT NOT NULL,
            status TEXT NOT NULL,
            payload_json TEXT,
            attempts INTEGER NOT NULL DEFAULT 0,
            available_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            lease_owner TEXT,
            lease_expires_at TEXT,
            queued_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            started_at TEXT,
            finished_at TEXT,
            message TEXT,
            UNIQUE (source_id, collection_id, task_kind, idempotency_key)
        );
        "#,
    )
    .context("initializing sqlite control-plane schema")?;

    Ok(conn)
}

async fn healthz(State(state): State<AppState>) -> Json<HealthResponse> {
    let sqlite_path = {
        let db = state.db.lock().expect("sqlite mutex poisoned");
        db.path()
            .map(str::to_string)
            .unwrap_or_else(|| ":memory:".to_string())
    };

    Json(HealthResponse {
        status: "ok",
        source_count: state.source_plans.len(),
        sqlite_path,
    })
}

async fn list_sources(State(state): State<AppState>) -> Json<Vec<SourcePlan>> {
    Json((*state.source_plans).clone())
}

async fn control_plane() -> Json<ControlPlaneSummary> {
    Json(ControlPlaneSummary {
        tables: vec![
            "source_collections",
            "discovered_artifacts",
            "fetched_artifacts",
            "tasks",
        ],
        notes: vec![
            "SQLite is the operational truth for queueing and completion tracking.",
            "ClickHouse remains the analytical warehouse and raw/semantic/serving store.",
            "Dagster, if used, should call this service rather than own pipeline correctness state.",
        ],
    })
}

async fn enqueue_demo_tasks(State(state): State<AppState>) -> Json<Vec<EnqueueResponse>> {
    let mut responses = Vec::new();
    let db = state.db.lock().expect("sqlite mutex poisoned");

    for source in state.source_plans.iter() {
        for collection in &source.collections {
            for blueprint in &collection.task_blueprints {
                let task_id = format!(
                    "{}:{}:{}:{}",
                    source.descriptor.source_id,
                    collection.id,
                    task_kind_slug(&blueprint.kind),
                    "bootstrap"
                );
                let idempotency_key = "bootstrap".to_string();
                let changed = db
                    .execute(
                        r#"
                        INSERT OR IGNORE INTO tasks (
                            task_id, source_id, collection_id, task_kind, idempotency_key, status, payload_json
                        ) VALUES (?1, ?2, ?3, ?4, ?5, 'queued', ?6)
                        "#,
                        rusqlite::params![
                            task_id,
                            source.descriptor.source_id,
                            collection.id,
                            task_kind_slug(&blueprint.kind),
                            idempotency_key,
                            format!(
                                "{{\"max_concurrency\":{},\"queue\":\"{}\"}}",
                                blueprint.max_concurrency, blueprint.queue
                            )
                        ],
                    )
                    .expect("enqueue bootstrap task");

                responses.push(EnqueueResponse {
                    accepted: changed > 0,
                    task_id: format!(
                        "{}:{}:{}:{}",
                        source.descriptor.source_id,
                        collection.id,
                        task_kind_slug(&blueprint.kind),
                        "bootstrap"
                    ),
                });
            }
        }
    }

    Json(responses)
}

async fn list_tasks(State(state): State<AppState>) -> Json<Vec<TaskRecord>> {
    let db = state.db.lock().expect("sqlite mutex poisoned");
    let mut stmt = db
        .prepare(
            r#"
            SELECT task_id, source_id, collection_id, task_kind, idempotency_key, status,
                   attempts, available_at, lease_owner, lease_expires_at
            FROM tasks
            ORDER BY queued_at DESC, task_id
            "#,
        )
        .expect("prepare task listing");

    let rows = stmt
        .query_map([], |row| {
            Ok(TaskRecord {
                task_id: row.get(0)?,
                source_id: row.get(1)?,
                collection_id: row.get(2)?,
                task_kind: row.get(3)?,
                idempotency_key: row.get(4)?,
                status: row.get(5)?,
                attempts: row.get(6)?,
                available_at: row.get(7)?,
                lease_owner: row.get(8)?,
                lease_expires_at: row.get(9)?,
            })
        })
        .expect("query task listing");

    Json(rows.map(|row| row.expect("task row")).collect())
}

fn task_kind_slug(kind: &TaskKind) -> &'static str {
    match kind {
        TaskKind::Discover => "discover",
        TaskKind::Fetch => "fetch",
        TaskKind::Parse => "parse",
        TaskKind::RegisterSchema => "register_schema",
        TaskKind::ReconcileRawStorage => "reconcile_raw_storage",
        TaskKind::ReconcileSemanticViews => "reconcile_semantic_views",
        TaskKind::Promote => "promote",
        TaskKind::SyncMetadata => "sync_metadata",
    }
}
