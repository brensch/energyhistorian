//! Background scheduler: claims tasks from SQLite, dispatches to source
//! plugins, and hands results to the pipeline for recording.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use ingest_core::{
    ArtifactKind, ArtifactMetadata, DiscoveredArtifact, FileSchemaRegistry, LocalArtifact,
};
use rusqlite::Connection;
use source_nemweb::NemwebPlugin;

use crate::pipeline;
use crate::warehouse::{ClickHouseConfig, ClickHousePublisher};

const LEASE_SECS: i64 = 300;
const POLL_INTERVAL: Duration = Duration::from_secs(2);
const MAX_CONCURRENT: usize = 8;
const DISCOVER_LIMIT: usize = 10;
const MAX_RETRIES: i64 = 3;
const RETRY_DELAY_SECS: i64 = 30;
const REDISCOVER_DELAY_SECS: i64 = 900;

#[derive(Debug, Clone)]
struct TaskRow {
    task_id: String,
    source_id: String,
    collection_id: String,
    task_kind: String,
    payload_json: Option<String>,
    attempts: i64,
}

pub async fn run(
    db: Arc<Mutex<Connection>>,
    raw_dir: PathBuf,
    parsed_dir: PathBuf,
    schema_registry_dir: PathBuf,
    clickhouse: ClickHouseConfig,
) {
    let worker_id = format!("w-{}", std::process::id());
    let client = reqwest::Client::builder()
        .user_agent("energyhistorian/0.1")
        .timeout(Duration::from_secs(60))
        .build()
        .expect("build HTTP client");
    let publisher = ClickHousePublisher::new(clickhouse).expect("build clickhouse publisher");
    let nemweb = NemwebPlugin::new();

    eprintln!("[scheduler] started id={worker_id}");

    loop {
        if let Err(e) = tick(
            &db,
            &worker_id,
            &client,
            &nemweb,
            &raw_dir,
            &parsed_dir,
            &schema_registry_dir,
            &publisher,
        )
        .await
        {
            eprintln!("[scheduler] tick error: {e:#}");
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

async fn tick(
    db: &Arc<Mutex<Connection>>,
    worker_id: &str,
    client: &reqwest::Client,
    nemweb: &NemwebPlugin,
    raw_dir: &Path,
    parsed_dir: &Path,
    schema_dir: &Path,
    publisher: &ClickHousePublisher,
) -> Result<()> {
    recover_expired_leases(db);

    let running = count_running(db);
    let slots = MAX_CONCURRENT.saturating_sub(running);

    for _ in 0..slots {
        let Some(task) = claim_next(db, worker_id) else {
            break;
        };

        eprintln!(
            "[scheduler] claimed {} ({}/{}:{})",
            task.task_id, task.source_id, task.collection_id, task.task_kind
        );

        let db2 = db.clone();
        let client2 = client.clone();
        let nemweb2 = nemweb.clone();
        let raw2 = raw_dir.to_path_buf();
        let parsed2 = parsed_dir.to_path_buf();
        let schema2 = schema_dir.to_path_buf();
        let publisher2 = publisher.clone();

        tokio::spawn(async move {
            let result = execute_task(
                &task,
                &client2,
                &nemweb2,
                &db2,
                &raw2,
                &parsed2,
                &schema2,
                &publisher2,
            )
            .await;

            match result {
                Ok(msg) => {
                    eprintln!("[scheduler] OK {}: {msg}", task.task_id);
                    mark_completed(&db2, &task.task_id, &msg);
                }
                Err(e) => {
                    let msg = format!("{e:#}");
                    eprintln!("[scheduler] FAIL {}: {msg}", task.task_id);
                    mark_failed(&db2, &task.task_id, task.attempts, &msg);
                }
            }
        });
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Task lifecycle (claim, complete, fail, recover)
// ---------------------------------------------------------------------------

fn recover_expired_leases(db: &Arc<Mutex<Connection>>) {
    let conn = db.lock().expect("mutex");
    let now = Utc::now().to_rfc3339();
    let n = conn
        .execute(
            "UPDATE tasks SET status = 'queued', lease_owner = NULL, lease_expires_at = NULL \
             WHERE status = 'running' AND lease_expires_at < ?1",
            rusqlite::params![now],
        )
        .unwrap_or(0);
    if n > 0 {
        eprintln!("[scheduler] recovered {n} expired lease(s)");
    }
}

fn count_running(db: &Arc<Mutex<Connection>>) -> usize {
    let conn = db.lock().expect("mutex");
    conn.query_row(
        "SELECT COUNT(*) FROM tasks WHERE status = 'running'",
        [],
        |row| row.get::<_, i64>(0),
    )
    .unwrap_or(0) as usize
}

fn claim_next(db: &Arc<Mutex<Connection>>, worker_id: &str) -> Option<TaskRow> {
    let conn = db.lock().expect("mutex");
    let now = Utc::now().to_rfc3339();
    let lease_until = (Utc::now() + chrono::Duration::seconds(LEASE_SECS)).to_rfc3339();

    let task = conn
        .query_row(
            "SELECT task_id, source_id, collection_id, task_kind, payload_json, attempts \
             FROM tasks \
             WHERE status = 'queued' AND available_at <= ?1 \
             ORDER BY queued_at ASC \
             LIMIT 1",
            rusqlite::params![now],
            |row| {
                Ok(TaskRow {
                    task_id: row.get(0)?,
                    source_id: row.get(1)?,
                    collection_id: row.get(2)?,
                    task_kind: row.get(3)?,
                    payload_json: row.get(4)?,
                    attempts: row.get(5)?,
                })
            },
        )
        .ok()?;

    let claimed = conn
        .execute(
            "UPDATE tasks SET status = 'running', lease_owner = ?1, lease_expires_at = ?2, \
             started_at = ?3, attempts = attempts + 1 \
             WHERE task_id = ?4 AND status = 'queued'",
            rusqlite::params![worker_id, lease_until, now, task.task_id],
        )
        .unwrap_or(0);

    if claimed > 0 { Some(task) } else { None }
}

fn mark_completed(db: &Arc<Mutex<Connection>>, task_id: &str, message: &str) {
    let conn = db.lock().expect("mutex");
    conn.execute(
        "UPDATE tasks SET status = 'completed', finished_at = ?1, message = ?2, \
         lease_owner = NULL, lease_expires_at = NULL \
         WHERE task_id = ?3",
        rusqlite::params![Utc::now().to_rfc3339(), message, task_id],
    )
    .ok();
}

fn mark_failed(db: &Arc<Mutex<Connection>>, task_id: &str, attempts: i64, message: &str) {
    let conn = db.lock().expect("mutex");
    if attempts < MAX_RETRIES {
        let retry_at = (Utc::now() + chrono::Duration::seconds(RETRY_DELAY_SECS)).to_rfc3339();
        conn.execute(
            "UPDATE tasks SET status = 'queued', available_at = ?1, \
             message = ?2, lease_owner = NULL, lease_expires_at = NULL \
             WHERE task_id = ?3",
            rusqlite::params![
                retry_at,
                format!("retry {}: {}", attempts + 1, message),
                task_id
            ],
        )
        .ok();
    } else {
        conn.execute(
            "UPDATE tasks SET status = 'failed', finished_at = ?1, message = ?2, \
             lease_owner = NULL, lease_expires_at = NULL \
             WHERE task_id = ?3",
            rusqlite::params![Utc::now().to_rfc3339(), message, task_id],
        )
        .ok();
    }
}

// ---------------------------------------------------------------------------
// Task dispatch — source-specific calls, then hand to pipeline
// ---------------------------------------------------------------------------

async fn execute_task(
    task: &TaskRow,
    client: &reqwest::Client,
    nemweb: &NemwebPlugin,
    db: &Arc<Mutex<Connection>>,
    raw_dir: &Path,
    parsed_dir: &Path,
    schema_dir: &Path,
    publisher: &ClickHousePublisher,
) -> Result<String> {
    match task.task_kind.as_str() {
        "discover" => exec_discover(task, client, nemweb, db).await,
        "fetch" => exec_fetch(task, client, nemweb, db, raw_dir).await,
        "parse" => exec_parse(task, nemweb, db, parsed_dir, schema_dir, publisher).await,
        kind => anyhow::bail!("no executor for task kind '{kind}'"),
    }
}

async fn exec_discover(
    task: &TaskRow,
    client: &reqwest::Client,
    nemweb: &NemwebPlugin,
    db: &Arc<Mutex<Connection>>,
) -> Result<String> {
    // Source-specific: get artifacts
    let artifacts = match task.source_id.as_str() {
        "aemo.nemweb" => nemweb
            .discover_collection(client, &task.collection_id, DISCOVER_LIMIT)
            .await
            .context("nemweb discover")?,
        src => anyhow::bail!("no discover implementation for source '{src}'"),
    };

    // Generic: hand to pipeline
    let discoveries: Vec<pipeline::Discovery> = artifacts
        .iter()
        .map(|a| pipeline::Discovery {
            artifact_id: stable_artifact_id(&task.source_id, &a.metadata.acquisition_uri),
            remote_uri: a.metadata.acquisition_uri.clone(),
        })
        .collect();

    let conn = db.lock().expect("mutex");
    let outcome =
        pipeline::record_discoveries(&conn, &task.source_id, &task.collection_id, &discoveries);
    pipeline::schedule_next_discovery(
        &conn,
        &task.source_id,
        &task.collection_id,
        REDISCOVER_DELAY_SECS,
    );

    Ok(outcome.to_string())
}

async fn exec_fetch(
    task: &TaskRow,
    client: &reqwest::Client,
    nemweb: &NemwebPlugin,
    db: &Arc<Mutex<Connection>>,
    raw_dir: &Path,
) -> Result<String> {
    let payload = parse_payload(&task.payload_json)?;
    let artifact_id = require_str(&payload, "artifact_id")?;
    let remote_uri = require_str(&payload, "remote_uri")?;

    // Generic: skip if already done
    {
        let conn = db.lock().expect("mutex");
        if pipeline::is_fetched(&conn, artifact_id) {
            return Ok(format!("{artifact_id} already fetched, skipping"));
        }
    }

    // Source-specific: download the artifact
    let local = match task.source_id.as_str() {
        "aemo.nemweb" => {
            let discovered = DiscoveredArtifact {
                metadata: ArtifactMetadata {
                    artifact_id: artifact_id.to_string(),
                    source_id: task.collection_id.clone(),
                    acquisition_uri: remote_uri.to_string(),
                    discovered_at: Utc::now(),
                    fetched_at: None,
                    published_at: None,
                    content_sha256: None,
                    content_length_bytes: None,
                    kind: ArtifactKind::ZipArchive,
                    parser_version: "source-nemweb/0.1".to_string(),
                    model_version: None,
                    release_name: None,
                },
            };
            let dir = raw_dir.join(&task.collection_id);
            std::fs::create_dir_all(&dir)?;
            nemweb
                .fetch_artifact(client, &discovered, &dir)
                .await
                .context("downloading archive")?
        }
        src => anyhow::bail!("no fetch implementation for source '{src}'"),
    };

    // Generic: record and chain
    let conn = db.lock().expect("mutex");
    let outcome = pipeline::record_fetch(
        &conn,
        &task.source_id,
        &task.collection_id,
        artifact_id,
        remote_uri,
        &local,
    );

    Ok(format!("{artifact_id}: {outcome}"))
}

async fn exec_parse(
    task: &TaskRow,
    nemweb: &NemwebPlugin,
    db: &Arc<Mutex<Connection>>,
    parsed_dir: &Path,
    schema_dir: &Path,
    publisher: &ClickHousePublisher,
) -> Result<String> {
    let payload = parse_payload(&task.payload_json)?;
    let artifact_id = require_str(&payload, "artifact_id")?;
    let local_path = require_str(&payload, "local_path")?;
    let remote_uri = payload["remote_uri"].as_str().unwrap_or("");

    // Source-specific: parse the artifact
    let result = match task.source_id.as_str() {
        "aemo.nemweb" => {
            let artifact = LocalArtifact {
                metadata: ArtifactMetadata {
                    artifact_id: artifact_id.to_string(),
                    source_id: task.collection_id.clone(),
                    acquisition_uri: remote_uri.to_string(),
                    discovered_at: Utc::now(),
                    fetched_at: Some(Utc::now()),
                    published_at: None,
                    content_sha256: None,
                    content_length_bytes: None,
                    kind: ArtifactKind::ZipArchive,
                    parser_version: "source-nemweb/0.1".to_string(),
                    model_version: None,
                    release_name: None,
                },
                local_path: PathBuf::from(local_path),
            };
            let dir = parsed_dir.join(&task.collection_id);
            std::fs::create_dir_all(&dir)?;
            nemweb
                .parse_artifact(&artifact, &dir)
                .context("parsing archive")?
        }
        src => anyhow::bail!("no parse implementation for source '{src}'"),
    };

    let published_rows = publisher
        .publish_parse_result(
            &task.source_id,
            &task.collection_id,
            artifact_id,
            remote_uri,
            &result,
        )
        .await
        .context("publishing parsed rows to clickhouse")?;

    // Generic: record schemas and update status
    let registry = FileSchemaRegistry::new(schema_dir.join("nemweb.schemas.json"));
    let conn = db.lock().expect("mutex");
    let outcome = pipeline::record_parse(
        &conn,
        &task.source_id,
        &task.collection_id,
        artifact_id,
        &result,
        &registry,
    )?;

    Ok(format!(
        "{artifact_id}: {outcome}; clickhouse rows={published_rows}"
    ))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn stable_artifact_id(source_id: &str, url: &str) -> String {
    let filename = url.rsplit('/').next().unwrap_or(url);
    let name = filename.strip_suffix(".zip").unwrap_or(filename);
    format!("{source_id}:{name}")
}

fn parse_payload(json: &Option<String>) -> Result<serde_json::Value> {
    json.as_deref()
        .map(serde_json::from_str)
        .transpose()
        .context("invalid task payload JSON")
        .map(|v| v.unwrap_or(serde_json::Value::Null))
}

fn require_str<'a>(payload: &'a serde_json::Value, field: &str) -> Result<&'a str> {
    payload[field]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("task payload missing required field '{field}'"))
}
