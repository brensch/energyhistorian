mod pipeline;
mod schedule_state;
mod scheduler;
mod warehouse;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use axum::extract::State;
use axum::routing::{get, post};
use axum::{Json, Router};
use clap::Parser;
use ingest_core::{SourceCollection, SourceDescriptor, SourceMetadataDocument, SourcePlugin};
use rusqlite::Connection;
use serde::Serialize;
use source_aemo_dvd::AemoDvdPlugin;
use source_aemo_metadata::AemoMetadataPlugin;
use source_nemweb::NemwebPlugin;
use tracing::info;
use tracing_subscriber::EnvFilter;
use warehouse::ClickHouseConfig;

#[derive(Parser, Debug)]
#[command(author, version, about = "energyhistorian daemon service")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:8080")]
    listen_addr: SocketAddr,
    #[arg(long, default_value = "data/control-plane.sqlite")]
    state_db: PathBuf,
    #[arg(long, env = "CLICKHOUSE_URL", default_value = "http://127.0.0.1:8123")]
    clickhouse_url: String,
    #[arg(long, env = "CLICKHOUSE_USER", default_value = "energyhistorian")]
    clickhouse_user: String,
    #[arg(long, env = "CLICKHOUSE_PASSWORD", default_value = "energyhistorian")]
    clickhouse_password: String,
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

#[derive(Debug, Serialize)]
struct CollectionScheduleResponse {
    source_id: String,
    collection_id: String,
    display_name: String,
    scheduler_enabled: bool,
    poll_interval_seconds: Option<u64>,
    status: String,
    next_task_kind: Option<String>,
    next_run_at: Option<String>,
    active_task_id: Option<String>,
    active_task_kind: Option<String>,
    last_task_id: Option<String>,
    last_task_kind: Option<String>,
    last_run_started_at: Option<String>,
    last_run_finished_at: Option<String>,
    last_success_at: Option<String>,
    last_error_at: Option<String>,
    last_error: Option<String>,
    consecutive_failures: i64,
    queued_tasks: i64,
    running_tasks: i64,
    failed_tasks: i64,
}

#[derive(Debug, Serialize)]
struct ScheduleOverview {
    collections: Vec<CollectionScheduleResponse>,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_logging();
    let args = Args::parse();
    let source_plans = build_source_plans();
    let db = open_state_db(&args.state_db)?;
    let schedule_seeds = build_schedule_seeds(&source_plans);
    let synced = schedule_state::sync_collection_schedules(&db, &schedule_seeds);
    info!("synced collection schedule state rows={synced}");
    let recovered = pipeline::requeue_unpublished_artifacts(&db);
    if recovered > 0 {
        info!("requeued unpublished artifacts count={recovered}");
    }
    let bootstrapped = schedule_state::bootstrap_collection_discovery_tasks(&db, &schedule_seeds);
    info!("bootstrapped discovery tasks count={bootstrapped}");
    let clickhouse = ClickHouseConfig {
        url: args.clickhouse_url.clone(),
        user: args.clickhouse_user.clone(),
        password: args.clickhouse_password.clone(),
    };
    warehouse::ClickHousePublisher::new(clickhouse.clone())?
        .ensure_ready()
        .await
        .context("checking clickhouse connectivity")?;
    info!("clickhouse connectivity verified");

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
            clickhouse,
            schedule_seeds,
        )
        .await;
    });

    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/sources", get(list_sources))
        .route("/control-plane", get(control_plane))
        .route("/tasks", get(list_tasks))
        .route("/schedule", get(list_schedule))
        .route("/tasks/enqueue", post(enqueue_demo_tasks))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(args.listen_addr)
        .await
        .with_context(|| format!("binding {}", args.listen_addr))?;
    info!(listen_addr = %args.listen_addr, "energyhistoriand listening");
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

        CREATE TABLE IF NOT EXISTS artifact_publications (
            artifact_id TEXT PRIMARY KEY,
            source_id TEXT NOT NULL,
            collection_id TEXT NOT NULL,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            published_at TEXT,
            last_error TEXT
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

        CREATE TABLE IF NOT EXISTS collection_schedule_state (
            source_id TEXT NOT NULL,
            collection_id TEXT NOT NULL,
            scheduler_enabled INTEGER NOT NULL,
            status TEXT NOT NULL,
            next_task_kind TEXT,
            next_run_at TEXT,
            active_task_id TEXT,
            active_task_kind TEXT,
            last_task_id TEXT,
            last_task_kind TEXT,
            last_run_started_at TEXT,
            last_run_finished_at TEXT,
            last_success_at TEXT,
            last_error_at TEXT,
            last_error TEXT,
            consecutive_failures INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (source_id, collection_id)
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
            "artifact_publications",
            "tasks",
            "collection_schedule_state",
        ],
        notes: vec![
            "SQLite is the operational truth for queueing and completion tracking.",
            "ClickHouse remains the analytical warehouse and raw/semantic/serving store.",
            "Dagster, if used, should call this service rather than own pipeline correctness state.",
        ],
    })
}

async fn enqueue_demo_tasks(State(state): State<AppState>) -> Json<Vec<EnqueueResponse>> {
    let db = state.db.lock().expect("sqlite mutex poisoned");
    let seeds = build_schedule_seeds(&state.source_plans);
    let responses = seeds
        .into_iter()
        .filter(|seed| seed.scheduler_enabled)
        .map(|seed| {
            let task_id = format!("{}:{}:discover:bootstrap", seed.source_id, seed.collection_id);
            let changed = db
                .execute(
                    "INSERT OR IGNORE INTO tasks \
                     (task_id, source_id, collection_id, task_kind, idempotency_key, status, available_at) \
                     VALUES (?1, ?2, ?3, 'discover', 'bootstrap', 'queued', CURRENT_TIMESTAMP)",
                    rusqlite::params![task_id, seed.source_id, seed.collection_id],
                )
                .expect("enqueue bootstrap discover task");

            EnqueueResponse {
                accepted: changed > 0,
                task_id,
            }
        })
        .collect();

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

async fn list_schedule(State(state): State<AppState>) -> Json<ScheduleOverview> {
    let db = state.db.lock().expect("sqlite mutex poisoned");
    let snapshots = schedule_state::list_collection_schedules(&db);
    drop(db);

    let collections = snapshots
        .into_iter()
        .map(|snapshot| {
            let plan = state
                .source_plans
                .iter()
                .find(|plan| plan.descriptor.source_id == snapshot.source_id);
            let collection = plan.and_then(|plan| {
                plan.collections
                    .iter()
                    .find(|collection| collection.id == snapshot.collection_id)
            });

            CollectionScheduleResponse {
                source_id: snapshot.source_id,
                collection_id: snapshot.collection_id,
                display_name: collection
                    .map(|collection| collection.display_name.clone())
                    .unwrap_or_else(|| "unknown".to_string()),
                scheduler_enabled: snapshot.scheduler_enabled,
                poll_interval_seconds: collection
                    .and_then(|collection| collection.default_poll_interval_seconds),
                status: snapshot.status,
                next_task_kind: snapshot.next_task_kind,
                next_run_at: snapshot.next_run_at,
                active_task_id: snapshot.active_task_id,
                active_task_kind: snapshot.active_task_kind,
                last_task_id: snapshot.last_task_id,
                last_task_kind: snapshot.last_task_kind,
                last_run_started_at: snapshot.last_run_started_at,
                last_run_finished_at: snapshot.last_run_finished_at,
                last_success_at: snapshot.last_success_at,
                last_error_at: snapshot.last_error_at,
                last_error: snapshot.last_error,
                consecutive_failures: snapshot.consecutive_failures,
                queued_tasks: snapshot.queued_tasks,
                running_tasks: snapshot.running_tasks,
                failed_tasks: snapshot.failed_tasks,
            }
        })
        .collect();

    Json(ScheduleOverview { collections })
}

fn build_schedule_seeds(
    source_plans: &[SourcePlan],
) -> Vec<schedule_state::CollectionScheduleSeed> {
    source_plans
        .iter()
        .flat_map(|source| {
            source
                .collections
                .iter()
                .map(|collection| schedule_state::CollectionScheduleSeed {
                    source_id: source.descriptor.source_id.clone(),
                    collection_id: collection.id.clone(),
                    poll_interval_seconds: collection.default_poll_interval_seconds,
                    scheduler_enabled: source.descriptor.source_id == "aemo.nemweb",
                })
        })
        .collect()
}

fn init_logging() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(true)
        .compact()
        .init();
}
