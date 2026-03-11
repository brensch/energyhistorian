//! Background scheduler: claims tasks from SQLite, dispatches to source
//! plugins, and hands results to the pipeline for recording.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use ingest_core::{
    ArtifactKind, ArtifactMetadata, DiscoveredArtifact, FileSchemaRegistry, LocalArtifact,
    ObservedSchema, RawPluginParseResult, RawTableRow, RawTableRowSink, RunContext, SourcePlugin,
};
use rusqlite::Connection;
use source_aemo_dvd::AemoMetadataDvdPlugin;
use source_aemo_metadata::AemoMetadataHtmlPlugin;
use source_nemweb::NemwebPlugin;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::pipeline;
use crate::schedule_state::{self, CollectionScheduleSeed};
use crate::warehouse::{ClickHouseConfig, ClickHousePublisher, MAX_INSERT_BYTES, MAX_INSERT_ROWS};

const LEASE_SECS: i64 = 300;
const POLL_INTERVAL: Duration = Duration::from_secs(2);
const MAX_CONCURRENT: usize = 8;
const DISCOVER_LIMIT: usize = 10;
const MAX_RETRIES: i64 = 3;
const RETRY_DELAY_SECS: i64 = 30;
const DEFAULT_REDISCOVER_DELAY_SECS: i64 = 900;

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
    schedule_seeds: Vec<CollectionScheduleSeed>,
) {
    let worker_id = format!("w-{}", std::process::id());
    let client = reqwest::Client::builder()
        .user_agent("energyhistorian/0.1")
        .timeout(Duration::from_secs(60))
        .build()
        .expect("build HTTP client");
    let publisher = ClickHousePublisher::new(clickhouse).expect("build clickhouse publisher");
    let nemweb = NemwebPlugin::new();
    let metadata_html = AemoMetadataHtmlPlugin::new();
    let metadata_dvd = AemoMetadataDvdPlugin::new();

    info!(worker_id = %worker_id, "scheduler started");

    loop {
        if let Err(e) = tick(
            &db,
            &worker_id,
            &client,
            &nemweb,
            &metadata_html,
            &metadata_dvd,
            &raw_dir,
            &parsed_dir,
            &schema_registry_dir,
            &publisher,
            &schedule_seeds,
        )
        .await
        {
            error!(error = ?e, "scheduler tick error");
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

async fn tick(
    db: &Arc<Mutex<Connection>>,
    worker_id: &str,
    client: &reqwest::Client,
    nemweb: &NemwebPlugin,
    metadata_html: &AemoMetadataHtmlPlugin,
    metadata_dvd: &AemoMetadataDvdPlugin,
    raw_dir: &Path,
    parsed_dir: &Path,
    schema_dir: &Path,
    publisher: &ClickHousePublisher,
    schedule_seeds: &[CollectionScheduleSeed],
) -> Result<()> {
    recover_expired_leases(db);

    let running = count_running(db);
    let slots = MAX_CONCURRENT.saturating_sub(running);

    for _ in 0..slots {
        let Some(task) = claim_next(db, worker_id) else {
            break;
        };

        info!(
            task_id = %task.task_id,
            source_id = %task.source_id,
            collection_id = %task.collection_id,
            task_kind = %task.task_kind,
            "claimed task"
        );

        let db2 = db.clone();
        let client2 = client.clone();
        let nemweb2 = nemweb.clone();
        let metadata_html2 = metadata_html.clone();
        let metadata_dvd2 = metadata_dvd.clone();
        let raw2 = raw_dir.to_path_buf();
        let parsed2 = parsed_dir.to_path_buf();
        let schema2 = schema_dir.to_path_buf();
        let publisher2 = publisher.clone();
        let schedule_seeds2 = schedule_seeds.to_vec();

        tokio::spawn(async move {
            let result = execute_task(
                &task,
                &client2,
                &nemweb2,
                &metadata_html2,
                &metadata_dvd2,
                &db2,
                &raw2,
                &parsed2,
                &schema2,
                &publisher2,
                &schedule_seeds2,
            )
            .await;

            match result {
                Ok(msg) => {
                    info!(task_id = %task.task_id, message = %msg, "task completed");
                    mark_completed(&db2, &task.task_id, &msg);
                }
                Err(e) => {
                    let msg = format!("{e:#}");
                    error!(task_id = %task.task_id, error = %msg, "task failed");
                    mark_failed(
                        &db2,
                        &schedule_seeds2,
                        &task.task_id,
                        &task.source_id,
                        &task.collection_id,
                        &task.task_kind,
                        task.attempts,
                        &msg,
                    );
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
    let mut expired = Vec::new();
    if let Ok(mut stmt) = conn.prepare(
        "SELECT source_id, collection_id, task_kind \
         FROM tasks \
         WHERE status = 'running' AND lease_expires_at < ?1",
    ) {
        if let Ok(rows) = stmt.query_map(rusqlite::params![now.clone()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        }) {
            expired.extend(rows.filter_map(|row| row.ok()));
        }
    }
    let n = conn
        .execute(
            "UPDATE tasks SET status = 'queued', lease_owner = NULL, lease_expires_at = NULL \
             WHERE status = 'running' AND lease_expires_at < ?1",
            rusqlite::params![now],
        )
        .unwrap_or(0);
    for (source_id, collection_id, task_kind) in expired {
        schedule_state::note_lease_recovered(&conn, &source_id, &collection_id, &task_kind);
    }
    if n > 0 {
        warn!(expired_leases = n, "recovered expired task leases");
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

    if claimed > 0 {
        schedule_state::note_task_started(
            &conn,
            &task.source_id,
            &task.collection_id,
            &task.task_id,
            &task.task_kind,
        );
        Some(task)
    } else {
        None
    }
}

fn mark_completed(db: &Arc<Mutex<Connection>>, task_id: &str, message: &str) {
    let conn = db.lock().expect("mutex");
    let task_meta: Option<(String, String)> = conn
        .query_row(
            "SELECT source_id, collection_id FROM tasks WHERE task_id = ?1",
            rusqlite::params![task_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .ok();
    conn.execute(
        "UPDATE tasks SET status = 'completed', finished_at = ?1, message = ?2, \
         lease_owner = NULL, lease_expires_at = NULL \
         WHERE task_id = ?3",
        rusqlite::params![Utc::now().to_rfc3339(), message, task_id],
    )
    .ok();
    if let Some((source_id, collection_id)) = task_meta {
        schedule_state::note_task_completed(&conn, &source_id, &collection_id);
    }
}

fn mark_failed(
    db: &Arc<Mutex<Connection>>,
    schedule_seeds: &[CollectionScheduleSeed],
    task_id: &str,
    source_id: &str,
    collection_id: &str,
    task_kind: &str,
    attempts: i64,
    message: &str,
) {
    let conn = db.lock().expect("mutex");
    if attempts < MAX_RETRIES {
        let retry_at = retry_at_for_task(
            schedule_seeds,
            source_id,
            collection_id,
            task_kind,
            Utc::now(),
        );
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
        schedule_state::note_task_failed(
            &conn,
            source_id,
            collection_id,
            task_kind,
            message,
            Some(&retry_at),
        );
    } else {
        conn.execute(
            "UPDATE tasks SET status = 'failed', finished_at = ?1, message = ?2, \
             lease_owner = NULL, lease_expires_at = NULL \
             WHERE task_id = ?3",
            rusqlite::params![Utc::now().to_rfc3339(), message, task_id],
        )
        .ok();
        schedule_state::note_task_failed(&conn, source_id, collection_id, task_kind, message, None);
    }
}

// ---------------------------------------------------------------------------
// Task dispatch — source-specific calls, then hand to pipeline
// ---------------------------------------------------------------------------

async fn execute_task(
    task: &TaskRow,
    client: &reqwest::Client,
    nemweb: &NemwebPlugin,
    metadata_html: &AemoMetadataHtmlPlugin,
    metadata_dvd: &AemoMetadataDvdPlugin,
    db: &Arc<Mutex<Connection>>,
    raw_dir: &Path,
    parsed_dir: &Path,
    schema_dir: &Path,
    publisher: &ClickHousePublisher,
    schedule_seeds: &[CollectionScheduleSeed],
) -> Result<String> {
    match task.task_kind.as_str() {
        "discover" => {
            exec_discover(
                task,
                client,
                nemweb,
                metadata_html,
                metadata_dvd,
                db,
                schedule_seeds,
            )
            .await
        }
        "fetch" => {
            exec_fetch(
                task,
                client,
                nemweb,
                metadata_html,
                metadata_dvd,
                db,
                raw_dir,
            )
            .await
        }
        "parse" => {
            exec_parse(
                task,
                nemweb,
                metadata_html,
                metadata_dvd,
                db,
                parsed_dir,
                schema_dir,
                publisher,
            )
            .await
        }
        kind => anyhow::bail!("no executor for task kind '{kind}'"),
    }
}

async fn exec_discover(
    task: &TaskRow,
    client: &reqwest::Client,
    nemweb: &NemwebPlugin,
    metadata_html: &AemoMetadataHtmlPlugin,
    metadata_dvd: &AemoMetadataDvdPlugin,
    db: &Arc<Mutex<Connection>>,
    schedule_seeds: &[CollectionScheduleSeed],
) -> Result<String> {
    // Source-specific: get artifacts
    let artifacts = match task.source_id.as_str() {
        "aemo.nemweb" => nemweb
            .discover_collection(client, &task.collection_id, DISCOVER_LIMIT)
            .await
            .context("nemweb discover")?,
        "aemo_metadata_html" => {
            let ctx = RunContext {
                run_id: format!(
                    "discover-{}-{}",
                    task.collection_id,
                    Utc::now().timestamp_millis()
                ),
                environment: "service".to_string(),
                parser_version: "source-aemo-metadata/0.1".to_string(),
            };
            metadata_html
                .discover_collection(client, &task.collection_id, &ctx)
                .await
                .context("html metadata discover")?
        }
        "aemo_metadata_dvd" => {
            let ctx = RunContext {
                run_id: format!(
                    "discover-{}-{}",
                    task.collection_id,
                    Utc::now().timestamp_millis()
                ),
                environment: "service".to_string(),
                parser_version: "source-aemo-dvd/0.1".to_string(),
            };
            metadata_dvd
                .discover_collection(client, &task.collection_id, &ctx)
                .await
                .context("dvd metadata discover")?
        }
        src => anyhow::bail!("no discover implementation for source '{src}'"),
    };

    // Generic: hand to pipeline
    let discoveries: Vec<pipeline::Discovery> = artifacts
        .iter()
        .map(|a| -> Result<pipeline::Discovery> {
            Ok(pipeline::Discovery {
                artifact_id: a.metadata.artifact_id.clone(),
                remote_uri: a.metadata.acquisition_uri.clone(),
                artifact_metadata: serde_json::to_value(&a.metadata)
                    .context("serializing discovered artifact metadata")?,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let conn = db.lock().expect("mutex");
    let outcome =
        pipeline::record_discoveries(&conn, &task.source_id, &task.collection_id, &discoveries);
    let next_run_at =
        collection_next_discovery_at(schedule_seeds, &task.source_id, &task.collection_id);
    let scheduled = pipeline::schedule_next_discovery(
        &conn,
        &task.source_id,
        &task.collection_id,
        &next_run_at,
    );
    schedule_state::note_discover_scheduled(
        &conn,
        &task.source_id,
        &task.collection_id,
        &scheduled,
    );

    Ok(outcome.to_string())
}

async fn exec_fetch(
    task: &TaskRow,
    client: &reqwest::Client,
    nemweb: &NemwebPlugin,
    metadata_html: &AemoMetadataHtmlPlugin,
    metadata_dvd: &AemoMetadataDvdPlugin,
    db: &Arc<Mutex<Connection>>,
    raw_dir: &Path,
) -> Result<String> {
    let payload = parse_payload(&task.payload_json)?;
    let artifact_id = require_str(&payload, "artifact_id")?;
    let remote_uri = require_str(&payload, "remote_uri")?;
    let discovered: ArtifactMetadata =
        artifact_metadata_from_payload(&payload, task, artifact_id, remote_uri)?;

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
                metadata: discovered,
            };
            let dir = raw_dir.join(&task.collection_id);
            std::fs::create_dir_all(&dir)?;
            nemweb
                .fetch_artifact(client, &discovered, &dir)
                .await
                .context("downloading archive")?
        }
        "aemo_metadata_html" => {
            let discovered = DiscoveredArtifact {
                metadata: discovered,
            };
            let dir = raw_dir.join("aemo_metadata_html").join(&task.collection_id);
            std::fs::create_dir_all(&dir)?;
            metadata_html
                .fetch_artifact(client, &discovered, &dir)
                .await
                .context("downloading html metadata artifact")?
        }
        "aemo_metadata_dvd" => {
            let discovered = DiscoveredArtifact {
                metadata: discovered,
            };
            let dir = raw_dir.join("aemo_metadata_dvd").join(&task.collection_id);
            std::fs::create_dir_all(&dir)?;
            metadata_dvd
                .fetch_artifact(client, &discovered, &dir)
                .await
                .context("downloading dvd metadata artifact")?
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
    metadata_html: &AemoMetadataHtmlPlugin,
    metadata_dvd: &AemoMetadataDvdPlugin,
    db: &Arc<Mutex<Connection>>,
    _parsed_dir: &Path,
    schema_dir: &Path,
    publisher: &ClickHousePublisher,
) -> Result<String> {
    let payload = parse_payload(&task.payload_json)?;
    let artifact_id = require_str(&payload, "artifact_id")?;
    let local_path = require_str(&payload, "local_path")?;
    let remote_uri = payload["remote_uri"].as_str().unwrap_or("");
    let artifact_metadata =
        artifact_metadata_from_payload(&payload, task, artifact_id, remote_uri)?;

    let ctx = RunContext {
        run_id: format!(
            "parse-{}-{}",
            task.collection_id,
            Utc::now().timestamp_millis()
        ),
        environment: "service".to_string(),
        parser_version: if task.source_id == "aemo_metadata_html" {
            "source-aemo-metadata/0.1".to_string()
        } else if task.source_id == "aemo_metadata_dvd" {
            "source-aemo-dvd/0.1".to_string()
        } else {
            "source-nemweb/0.1".to_string()
        },
    };

    // Source-specific: parse the artifact
    let parse_outcome = match task.source_id.as_str() {
        "aemo.nemweb" => {
            let artifact = LocalArtifact {
                metadata: artifact_metadata,
                local_path: PathBuf::from(local_path),
            };
            let inspected = nemweb
                .inspect_parse(&artifact, &ctx)
                .context("inspecting archive for schemas and counts")?;
            ParsedArtifact::Nemweb {
                artifact,
                result: inspected,
            }
        }
        "aemo_metadata_html" => {
            let artifact = LocalArtifact {
                metadata: artifact_metadata,
                local_path: PathBuf::from(local_path),
            };
            let result = metadata_html
                .parse_artifact(&task.collection_id, &artifact)
                .context("parsing html metadata artifact")?;
            ParsedArtifact::RawPluginMetadata { artifact, result }
        }
        "aemo_metadata_dvd" => {
            let artifact = LocalArtifact {
                metadata: artifact_metadata,
                local_path: PathBuf::from(local_path),
            };
            let result = metadata_dvd
                .parse_artifact(&artifact)
                .context("parsing dvd metadata artifact")?;
            ParsedArtifact::RawPluginMetadata { artifact, result }
        }
        src => anyhow::bail!("no parse implementation for source '{src}'"),
    };

    match parse_outcome {
        ParsedArtifact::Nemweb { artifact, result } => {
            publisher
                .ensure_tables(&result.observed_schemas)
                .await
                .context("preparing clickhouse raw tables")?;

            let (tx, mut rx) = mpsc::channel::<RawTableRow>(64);
            let artifact2 = artifact.clone();
            let ctx2 = ctx.clone();
            let nemweb2 = nemweb.clone();
            let parse_handle = tokio::task::spawn_blocking(move || -> Result<()> {
                let mut sink = ChannelRawTableRowSink { tx };
                nemweb2
                    .stream_parse(&artifact2, &ctx2, &mut sink)
                    .context("streaming parsed rows")
            });

            let processed_at = Utc::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string();
            let mut batcher = ClickHouseRowBatcher::new(
                publisher,
                &result.observed_schemas,
                &task.source_id,
                &task.collection_id,
                artifact_id,
                remote_uri,
                processed_at,
            );
            while let Some(row) = rx.recv().await {
                batcher.push(row).await?;
            }
            parse_handle.await.context("joining parser stream task")??;
            let published_rows = batcher.finish().await?;

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
        ParsedArtifact::RawPluginMetadata { artifact, result } => {
            let published_rows = publisher
                .publish_raw_plugin_parse_result(
                    &task.source_id,
                    &task.collection_id,
                    &artifact,
                    &result,
                )
                .await
                .context("publishing raw metadata rows")?;
            let conn = db.lock().expect("mutex");
            pipeline::record_publication(&conn, &task.source_id, &task.collection_id, artifact_id);
            Ok(format!(
                "{artifact_id}: raw metadata rows published={published_rows}"
            ))
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

struct ChannelRawTableRowSink {
    tx: mpsc::Sender<RawTableRow>,
}

impl RawTableRowSink for ChannelRawTableRowSink {
    fn accept(&mut self, row: RawTableRow) -> Result<()> {
        self.tx
            .blocking_send(row)
            .map_err(|_| anyhow!("row stream channel closed while parsing"))
    }
}

struct PendingInsertChunk {
    logical_table_key: String,
    schema_key: String,
    rows: Vec<serde_json::Value>,
    encoded_bytes: usize,
}

struct ClickHouseRowBatcher<'a> {
    publisher: &'a ClickHousePublisher,
    schemas_by_hash: HashMap<String, ObservedSchema>,
    source_id: &'a str,
    collection_id: &'a str,
    artifact_id: &'a str,
    remote_uri: &'a str,
    processed_at: String,
    pending: HashMap<(String, String), PendingInsertChunk>,
    published_rows: usize,
}

impl<'a> ClickHouseRowBatcher<'a> {
    fn new(
        publisher: &'a ClickHousePublisher,
        schemas: &[ObservedSchema],
        source_id: &'a str,
        collection_id: &'a str,
        artifact_id: &'a str,
        remote_uri: &'a str,
        processed_at: String,
    ) -> Self {
        Self {
            publisher,
            schemas_by_hash: schemas
                .iter()
                .map(|schema| (schema.schema_key.header_hash.clone(), schema.clone()))
                .collect(),
            source_id,
            collection_id,
            artifact_id,
            remote_uri,
            processed_at,
            pending: HashMap::new(),
            published_rows: 0,
        }
    }

    async fn push(&mut self, row: RawTableRow) -> Result<()> {
        let encoded_len = serde_json::to_string(&row.row)?.len() + 1;
        let key = (row.logical_table_key.clone(), row.schema_key.clone());
        if let Some(existing) = self.pending.get(&key) {
            let would_exceed_rows = existing.rows.len() >= MAX_INSERT_ROWS;
            let would_exceed_bytes = !existing.rows.is_empty()
                && existing.encoded_bytes + encoded_len > MAX_INSERT_BYTES;
            if would_exceed_rows || would_exceed_bytes {
                self.flush_key(&key).await?;
            }
        }

        let entry = self
            .pending
            .entry(key.clone())
            .or_insert_with(|| PendingInsertChunk {
                logical_table_key: row.logical_table_key,
                schema_key: row.schema_key,
                rows: Vec::new(),
                encoded_bytes: 0,
            });
        entry.encoded_bytes += encoded_len;
        entry.rows.push(row.row);

        if entry.rows.len() >= MAX_INSERT_ROWS || entry.encoded_bytes >= MAX_INSERT_BYTES {
            self.flush_key(&key).await?;
        }

        Ok(())
    }

    async fn finish(&mut self) -> Result<usize> {
        let keys = self.pending.keys().cloned().collect::<Vec<_>>();
        for key in keys {
            self.flush_key(&key).await?;
        }
        Ok(self.published_rows)
    }

    async fn flush_key(&mut self, key: &(String, String)) -> Result<()> {
        let Some(chunk) = self.pending.remove(key) else {
            return Ok(());
        };
        if chunk.rows.is_empty() {
            return Ok(());
        }
        let schema = self
            .schemas_by_hash
            .get(&chunk.schema_key)
            .ok_or_else(|| anyhow!("missing schema for chunk {}", chunk.logical_table_key))?;
        self.published_rows += self
            .publisher
            .publish_raw_chunk(
                schema,
                self.source_id,
                self.collection_id,
                self.artifact_id,
                self.remote_uri,
                &self.processed_at,
                &chunk.rows,
            )
            .await?;
        Ok(())
    }
}

fn parse_payload(json: &Option<String>) -> Result<serde_json::Value> {
    json.as_deref()
        .map(serde_json::from_str)
        .transpose()
        .context("invalid task payload JSON")
        .map(|v| v.unwrap_or(serde_json::Value::Null))
}

fn artifact_metadata_from_payload(
    payload: &serde_json::Value,
    task: &TaskRow,
    artifact_id: &str,
    remote_uri: &str,
) -> Result<ArtifactMetadata> {
    if !payload["artifact"].is_null() {
        return serde_json::from_value(payload["artifact"].clone())
            .context("deserializing artifact metadata from task payload");
    }

    Ok(ArtifactMetadata {
        artifact_id: artifact_id.to_string(),
        source_id: task.source_id.clone(),
        acquisition_uri: remote_uri.to_string(),
        discovered_at: Utc::now(),
        fetched_at: None,
        published_at: None,
        content_sha256: None,
        content_length_bytes: None,
        kind: if task.source_id == "aemo_metadata_html" {
            ArtifactKind::HtmlDocument
        } else if task.source_id == "aemo_metadata_dvd" {
            ArtifactKind::ZipArchive
        } else {
            ArtifactKind::ZipArchive
        },
        parser_version: if task.source_id == "aemo_metadata_html" {
            "source-aemo-metadata/0.1".to_string()
        } else if task.source_id == "aemo_metadata_dvd" {
            "source-aemo-dvd/0.1".to_string()
        } else {
            "source-nemweb/0.1".to_string()
        },
        model_version: None,
        release_name: None,
    })
}

enum ParsedArtifact {
    Nemweb {
        artifact: LocalArtifact,
        result: ingest_core::ParseResult,
    },
    RawPluginMetadata {
        artifact: LocalArtifact,
        result: RawPluginParseResult,
    },
}

fn require_str<'a>(payload: &'a serde_json::Value, field: &str) -> Result<&'a str> {
    payload[field]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("task payload missing required field '{field}'"))
}

fn collection_next_discovery_at(
    schedule_seeds: &[CollectionScheduleSeed],
    source_id: &str,
    collection_id: &str,
) -> String {
    collection_next_discovery_at_from(schedule_seeds, source_id, collection_id, Utc::now())
}

fn collection_next_discovery_at_from(
    schedule_seeds: &[CollectionScheduleSeed],
    source_id: &str,
    collection_id: &str,
    now: chrono::DateTime<Utc>,
) -> String {
    let seed = schedule_seeds
        .iter()
        .find(|seed| seed.source_id == source_id && seed.collection_id == collection_id);
    match seed {
        Some(seed) => schedule_state::next_discovery_time(seed, now).to_rfc3339(),
        None => (now + chrono::Duration::seconds(DEFAULT_REDISCOVER_DELAY_SECS)).to_rfc3339(),
    }
}

fn retry_at_for_task(
    schedule_seeds: &[CollectionScheduleSeed],
    source_id: &str,
    collection_id: &str,
    task_kind: &str,
    now: chrono::DateTime<Utc>,
) -> String {
    if task_kind == "discover" {
        return collection_next_discovery_at_from(schedule_seeds, source_id, collection_id, now);
    }

    (now + chrono::Duration::seconds(RETRY_DELAY_SECS)).to_rfc3339()
}
