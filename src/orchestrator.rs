use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use ingest_core::{
    ArtifactMetadata, DiscoveredArtifact, DiscoveryCursorHint, LocalArtifact, RawTableRow,
    RawTableRowSink,
};
use tokio::sync::{Semaphore, mpsc};
use tracing::{error, info, warn};

use crate::clickhouse::{ClickHousePublisher, MAX_INSERT_BYTES, MAX_INSERT_ROWS};
use crate::db::{Db, now_iso};
use crate::semantic::reconcile_source_semantics;
use crate::source_registry::{ParsedArtifact, ScheduleSeed, SourceRegistry};

#[derive(Debug, Clone)]
pub struct Stats {
    pub discovers_ok: Arc<AtomicU64>,
    pub discovers_err: Arc<AtomicU64>,
    pub downloads_ok: Arc<AtomicU64>,
    pub downloads_err: Arc<AtomicU64>,
    pub parses_ok: Arc<AtomicU64>,
    pub parses_err: Arc<AtomicU64>,
}

impl Default for Stats {
    fn default() -> Self {
        Self {
            discovers_ok: Arc::new(AtomicU64::new(0)),
            discovers_err: Arc::new(AtomicU64::new(0)),
            downloads_ok: Arc::new(AtomicU64::new(0)),
            downloads_err: Arc::new(AtomicU64::new(0)),
            parses_ok: Arc::new(AtomicU64::new(0)),
            parses_err: Arc::new(AtomicU64::new(0)),
        }
    }
}

pub async fn run_orchestrator(
    db: Db,
    registry: SourceRegistry,
    publisher: ClickHousePublisher,
    data_dir: PathBuf,
    discover_concurrency: usize,
    download_concurrency: usize,
    parse_concurrency: usize,
    stats: Stats,
) -> Result<()> {
    // Sync schedule seeds into SQLite on startup
    let seeds = registry.schedule_seeds();
    sync_schedules(&db, &seeds).await?;

    // Recover any interrupted downloads: if file is missing, reset to discovered
    recover_interrupted(&db, &data_dir).await?;

    let http_client = reqwest::Client::builder()
        .user_agent("energyhistorian/0.1")
        .timeout(Duration::from_secs(120))
        .build()?;

    let discover_sem = Arc::new(Semaphore::new(discover_concurrency.max(1)));
    let download_sem = Arc::new(Semaphore::new(download_concurrency.max(1)));
    let parse_sem = Arc::new(Semaphore::new(parse_concurrency.max(1)));

    let discover_handle = tokio::spawn({
        let db = db.clone();
        let registry = registry.clone();
        let http_client = http_client.clone();
        let sem = discover_sem.clone();
        let stats = stats.clone();
        async move {
            if let Err(e) = discover_loop(db, registry, http_client, sem, stats).await {
                error!(error = %e, "discover loop failed");
            }
        }
    });

    let download_handle = tokio::spawn({
        let db = db.clone();
        let registry = registry.clone();
        let http_client = http_client.clone();
        let sem = download_sem.clone();
        let data_dir = data_dir.clone();
        let stats = stats.clone();
        async move {
            if let Err(e) = download_loop(db, registry, http_client, sem, data_dir, stats).await {
                error!(error = %e, "download loop failed");
            }
        }
    });

    let parse_handle = tokio::spawn({
        let db = db.clone();
        let registry = registry.clone();
        let publisher = publisher.clone();
        let sem = parse_sem.clone();
        let stats = stats.clone();
        async move {
            if let Err(e) = parse_loop(db, registry, publisher, sem, stats).await {
                error!(error = %e, "parse loop failed");
            }
        }
    });

    tokio::select! {
        _ = discover_handle => warn!("discover loop exited"),
        _ = download_handle => warn!("download loop exited"),
        _ = parse_handle => warn!("parse loop exited"),
    }

    Ok(())
}

async fn sync_schedules(db: &Db, seeds: &[ScheduleSeed]) -> Result<()> {
    let conn = db.lock().await;
    let now = now_iso();
    for seed in seeds {
        conn.execute(
            "INSERT INTO schedules (source_id, collection_id, enabled, poll_interval_seconds, next_discovery_at)
             VALUES (?1, ?2, 1, ?3, ?4)
             ON CONFLICT (source_id, collection_id) DO UPDATE
             SET poll_interval_seconds = excluded.poll_interval_seconds",
            rusqlite::params![
                seed.source_id,
                seed.collection_id,
                seed.poll_interval_seconds,
                now,
            ],
        )?;
    }
    let count = seeds.len();
    info!(schedule_count = count, "synced schedules");
    Ok(())
}

async fn recover_interrupted(db: &Db, data_dir: &Path) -> Result<()> {
    let conn = db.lock().await;
    // Find downloaded artifacts where the file no longer exists
    let mut stmt = conn.prepare(
        "SELECT d.artifact_id, d.local_path FROM downloads d
         JOIN artifacts a ON a.artifact_id = d.artifact_id
         WHERE a.status = 'downloaded'",
    )?;
    let missing: Vec<String> = stmt
        .query_map([], |row| {
            let artifact_id: String = row.get(0)?;
            let local_path: String = row.get(1)?;
            Ok((artifact_id, local_path))
        })?
        .filter_map(|r| r.ok())
        .filter(|(_, path)| !Path::new(path).exists())
        .map(|(id, _)| id)
        .collect();
    drop(stmt);

    for artifact_id in &missing {
        conn.execute(
            "UPDATE artifacts SET status = 'discovered', updated_at = ?1 WHERE artifact_id = ?2",
            rusqlite::params![now_iso(), artifact_id],
        )?;
        conn.execute(
            "DELETE FROM downloads WHERE artifact_id = ?1",
            rusqlite::params![artifact_id],
        )?;
    }
    if !missing.is_empty() {
        info!(count = missing.len(), "recovered interrupted downloads");
    }
    let _ = data_dir; // used for context only
    Ok(())
}

// ── Discover Loop ──────────────────────────────────────────────────

async fn discover_loop(
    db: Db,
    registry: SourceRegistry,
    http_client: reqwest::Client,
    sem: Arc<Semaphore>,
    stats: Stats,
) -> Result<()> {
    loop {
        let due = find_due_schedules(&db).await?;
        if due.is_empty() {
            tokio::time::sleep(Duration::from_secs(5)).await;
            continue;
        }

        let mut handles = Vec::new();
        for (source_id, collection_id) in due {
            let permit = sem.clone().acquire_owned().await?;
            let db = db.clone();
            let registry = registry.clone();
            let http_client = http_client.clone();
            let stats = stats.clone();
            handles.push(tokio::spawn(async move {
                let result =
                    run_discover(&db, &registry, &http_client, &source_id, &collection_id).await;
                match &result {
                    Ok(count) => {
                        stats.discovers_ok.fetch_add(1, Ordering::Relaxed);
                        if *count > 0 {
                            info!(
                                source_id,
                                collection_id,
                                discovered = count,
                                "discovery complete"
                            );
                        }
                    }
                    Err(e) => {
                        stats.discovers_err.fetch_add(1, Ordering::Relaxed);
                        warn!(source_id, collection_id, error = %e, "discovery failed");
                    }
                }
                drop(permit);
            }));
        }
        for h in handles {
            let _ = h.await;
        }
    }
}

async fn find_due_schedules(db: &Db) -> Result<Vec<(String, String)>> {
    let conn = db.lock().await;
    let now = now_iso();
    let mut stmt = conn.prepare(
        "SELECT source_id, collection_id FROM schedules
         WHERE enabled = 1 AND next_discovery_at <= ?1",
    )?;
    let rows = stmt
        .query_map(rusqlite::params![now], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .filter_map(|r| r.ok())
        .collect();
    Ok(rows)
}

async fn run_discover(
    db: &Db,
    registry: &SourceRegistry,
    http_client: &reqwest::Client,
    source_id: &str,
    collection_id: &str,
) -> Result<usize> {
    let cursor = fetch_cursor_hint(db, source_id, collection_id).await?;
    let discoveries = registry
        .discover(http_client, source_id, collection_id, 10, &cursor)
        .await?;

    let now = now_iso();
    let count = discoveries.len();

    let conn = db.lock().await;
    for artifact in &discoveries {
        let metadata_json = serde_json::to_string(&artifact.metadata)?;
        let artifact_kind = format!("{:?}", artifact.metadata.kind);
        conn.execute(
            "INSERT INTO artifacts (artifact_id, source_id, collection_id, remote_uri, artifact_kind, metadata_json, discovered_at, publication_timestamp, status, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'discovered', ?9)
             ON CONFLICT (artifact_id) DO NOTHING",
            rusqlite::params![
                artifact.metadata.artifact_id,
                source_id,
                collection_id,
                artifact.metadata.acquisition_uri,
                artifact_kind,
                metadata_json,
                artifact.metadata.discovered_at.to_rfc3339(),
                artifact.metadata.published_at.map(|dt: DateTime<Utc>| dt.to_rfc3339()),
                now,
            ],
        )?;
    }

    // Update schedule next_discovery_at
    let poll_interval: i64 = conn.query_row(
        "SELECT poll_interval_seconds FROM schedules WHERE source_id = ?1 AND collection_id = ?2",
        rusqlite::params![source_id, collection_id],
        |row| row.get(0),
    )?;
    let next = Utc::now() + chrono::Duration::seconds(poll_interval);
    conn.execute(
        "UPDATE schedules SET next_discovery_at = ?1, last_discovery_at = ?2, last_success_at = ?2, consecutive_failures = 0
         WHERE source_id = ?3 AND collection_id = ?4",
        rusqlite::params![next.to_rfc3339(), now, source_id, collection_id],
    )?;

    Ok(count)
}

async fn fetch_cursor_hint(
    db: &Db,
    source_id: &str,
    collection_id: &str,
) -> Result<DiscoveryCursorHint> {
    let conn = db.lock().await;
    let result = conn.query_row(
        "SELECT publication_timestamp, metadata_json FROM artifacts
         WHERE source_id = ?1 AND collection_id = ?2
         ORDER BY publication_timestamp DESC NULLS LAST, discovered_at DESC
         LIMIT 1",
        rusqlite::params![source_id, collection_id],
        |row| {
            let pub_ts: Option<String> = row.get(0)?;
            let metadata_json: String = row.get(1)?;
            Ok((pub_ts, metadata_json))
        },
    );
    match result {
        Ok((pub_ts, metadata_json)) => {
            let metadata: ArtifactMetadata = serde_json::from_str(&metadata_json)?;
            let latest_publication_timestamp = pub_ts
                .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
                .map(|dt| dt.with_timezone(&Utc));
            Ok(DiscoveryCursorHint {
                latest_publication_timestamp,
                latest_release_name: metadata.release_name,
            })
        }
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(DiscoveryCursorHint::default()),
        Err(e) => Err(e.into()),
    }
}

// ── Download Loop ──────────────────────────────────────────────────

async fn download_loop(
    db: Db,
    registry: SourceRegistry,
    http_client: reqwest::Client,
    sem: Arc<Semaphore>,
    data_dir: PathBuf,
    stats: Stats,
) -> Result<()> {
    loop {
        let batch = find_downloadable(&db, 20).await?;
        if batch.is_empty() {
            tokio::time::sleep(Duration::from_secs(2)).await;
            continue;
        }

        let mut handles = Vec::new();
        for (artifact_id, source_id, collection_id, metadata_json) in batch {
            let permit = sem.clone().acquire_owned().await?;
            let db = db.clone();
            let registry = registry.clone();
            let http_client = http_client.clone();
            let data_dir = data_dir.clone();
            let stats = stats.clone();
            handles.push(tokio::spawn(async move {
                let result = run_download(
                    &db,
                    &registry,
                    &http_client,
                    &data_dir,
                    &artifact_id,
                    &source_id,
                    &collection_id,
                    &metadata_json,
                )
                .await;
                match &result {
                    Ok(_) => {
                        stats.downloads_ok.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        stats.downloads_err.fetch_add(1, Ordering::Relaxed);
                        warn!(artifact_id, error = %e, "download failed");
                        // Mark as failed so we don't retry endlessly
                        let conn = db.lock().await;
                        let _ = conn.execute(
                            "UPDATE artifacts SET status = 'download_failed', error_text = ?1, updated_at = ?2 WHERE artifact_id = ?3",
                            rusqlite::params![format!("{e:#}"), now_iso(), artifact_id],
                        );
                    }
                }
                drop(permit);
            }));
        }
        for h in handles {
            let _ = h.await;
        }
    }
}

async fn find_downloadable(db: &Db, limit: usize) -> Result<Vec<(String, String, String, String)>> {
    let conn = db.lock().await;
    let mut stmt = conn.prepare(
        "SELECT artifact_id, source_id, collection_id, metadata_json FROM artifacts
         WHERE status = 'discovered'
         ORDER BY discovered_at ASC
         LIMIT ?1",
    )?;
    let rows = stmt
        .query_map(rusqlite::params![limit], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?
        .filter_map(|r| r.ok())
        .collect();
    Ok(rows)
}

async fn run_download(
    db: &Db,
    registry: &SourceRegistry,
    http_client: &reqwest::Client,
    data_dir: &Path,
    artifact_id: &str,
    source_id: &str,
    collection_id: &str,
    metadata_json: &str,
) -> Result<()> {
    let metadata: ArtifactMetadata = serde_json::from_str(metadata_json)?;
    let discovered = DiscoveredArtifact {
        metadata: metadata.clone(),
    };

    // Download to a local directory
    let artifact_dir = data_dir
        .join("artifacts")
        .join(sanitize_path(source_id))
        .join(sanitize_path(collection_id))
        .join(sanitize_path(artifact_id));
    tokio::fs::create_dir_all(&artifact_dir).await?;

    let local = registry
        .fetch(
            http_client,
            source_id,
            collection_id,
            &discovered,
            &artifact_dir,
        )
        .await?;

    let local_path_str = local.local_path.display().to_string();
    let file_len = tokio::fs::metadata(&local.local_path)
        .await
        .map(|m| m.len() as i64)
        .unwrap_or(0);

    let now = now_iso();
    let conn = db.lock().await;
    conn.execute(
        "INSERT INTO downloads (artifact_id, local_path, content_sha256, content_length_bytes, downloaded_at)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT (artifact_id) DO UPDATE
         SET local_path = excluded.local_path, content_sha256 = excluded.content_sha256,
             content_length_bytes = excluded.content_length_bytes, downloaded_at = excluded.downloaded_at",
        rusqlite::params![
            artifact_id,
            local_path_str,
            metadata.content_sha256.as_deref().unwrap_or(""),
            file_len,
            now,
        ],
    )?;
    conn.execute(
        "UPDATE artifacts SET status = 'downloaded', updated_at = ?1 WHERE artifact_id = ?2",
        rusqlite::params![now, artifact_id],
    )?;

    info!(
        artifact_id,
        source_id,
        collection_id,
        bytes = file_len,
        "downloaded"
    );
    Ok(())
}

// ── Parse Loop ──────────────────────────────────────────────────

async fn parse_loop(
    db: Db,
    registry: SourceRegistry,
    publisher: ClickHousePublisher,
    sem: Arc<Semaphore>,
    stats: Stats,
) -> Result<()> {
    loop {
        let batch = find_parseable(&db, 10).await?;
        if batch.is_empty() {
            tokio::time::sleep(Duration::from_secs(2)).await;
            continue;
        }

        let mut handles = Vec::new();
        for (artifact_id, source_id, collection_id, metadata_json, local_path) in batch {
            let permit = sem.clone().acquire_owned().await?;
            let db = db.clone();
            let registry = registry.clone();
            let publisher = publisher.clone();
            let stats = stats.clone();
            handles.push(tokio::spawn(async move {
                let result = run_parse(
                    &db,
                    &registry,
                    &publisher,
                    &artifact_id,
                    &source_id,
                    &collection_id,
                    &metadata_json,
                    &local_path,
                )
                .await;
                match &result {
                    Ok(rows) => {
                        stats.parses_ok.fetch_add(1, Ordering::Relaxed);
                        info!(
                            artifact_id,
                            source_id,
                            collection_id,
                            rows,
                            "parsed"
                        );
                    }
                    Err(e) => {
                        stats.parses_err.fetch_add(1, Ordering::Relaxed);
                        warn!(artifact_id, error = %e, "parse failed");
                        let conn = db.lock().await;
                        let _ = conn.execute(
                            "UPDATE artifacts SET status = 'parse_failed', error_text = ?1, updated_at = ?2 WHERE artifact_id = ?3",
                            rusqlite::params![format!("{e:#}"), now_iso(), artifact_id],
                        );
                    }
                }
                drop(permit);
            }));
        }
        for h in handles {
            let _ = h.await;
        }
    }
}

async fn find_parseable(
    db: &Db,
    limit: usize,
) -> Result<Vec<(String, String, String, String, String)>> {
    let conn = db.lock().await;
    let mut stmt = conn.prepare(
        "SELECT a.artifact_id, a.source_id, a.collection_id, a.metadata_json, d.local_path
         FROM artifacts a
         JOIN downloads d ON d.artifact_id = a.artifact_id
         WHERE a.status = 'downloaded'
         AND NOT EXISTS (
             SELECT 1 FROM parse_runs p
             WHERE p.artifact_id = a.artifact_id AND p.status = 'succeeded'
         )
         ORDER BY a.discovered_at ASC
         LIMIT ?1",
    )?;
    let rows = stmt
        .query_map(rusqlite::params![limit], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
            ))
        })?
        .filter_map(|r| r.ok())
        .collect();
    Ok(rows)
}

async fn run_parse(
    db: &Db,
    registry: &SourceRegistry,
    publisher: &ClickHousePublisher,
    artifact_id: &str,
    source_id: &str,
    collection_id: &str,
    metadata_json: &str,
    local_path: &str,
) -> Result<usize> {
    let metadata: ArtifactMetadata = serde_json::from_str(metadata_json)?;
    let parser_version = registry.parser_version(source_id)?;

    // Check if already parsed with this version
    {
        let conn = db.lock().await;
        let already: bool = conn
            .query_row(
                "SELECT COUNT(*) FROM parse_runs WHERE artifact_id = ?1 AND parser_version = ?2 AND status = 'succeeded'",
                rusqlite::params![artifact_id, parser_version],
                |row| Ok(row.get::<_, i64>(0)? > 0),
            )
            .unwrap_or(false);
        if already {
            conn.execute(
                "UPDATE artifacts SET status = 'parsed', updated_at = ?1 WHERE artifact_id = ?2",
                rusqlite::params![now_iso(), artifact_id],
            )?;
            return Ok(0);
        }
    }

    let artifact_path = PathBuf::from(local_path);
    let artifact = LocalArtifact {
        metadata,
        local_path: artifact_path.clone(),
    };

    let parsed = registry.parse(source_id, collection_id, artifact.clone())?;
    let rows_published = match parsed {
        ParsedArtifact::StructuredRaw {
            artifact: ref local_artifact,
            ref result,
        } => {
            publish_structured_raw_rows(
                publisher,
                registry,
                source_id,
                collection_id,
                local_artifact,
                result,
            )
            .await?
        }
        ParsedArtifact::RawMetadata {
            artifact: ref local_artifact,
            ref result,
        } => {
            publisher
                .publish_raw_plugin_parse_result(source_id, collection_id, local_artifact, result)
                .await?
        }
    };

    // Reconcile semantic views
    let semantic_jobs = reconcile_source_semantics(publisher, registry, source_id).await?;

    // Record success
    let now = now_iso();
    let summary = serde_json::json!({
        "clickhouse_rows": rows_published,
        "semantic_jobs": semantic_jobs,
    });
    {
        let conn = db.lock().await;
        conn.execute(
            "INSERT INTO parse_runs (artifact_id, parser_version, status, row_count, summary_json, started_at, completed_at)
             VALUES (?1, ?2, 'succeeded', ?3, ?4, ?5, ?5)
             ON CONFLICT (artifact_id, parser_version) DO UPDATE
             SET status = 'succeeded', row_count = excluded.row_count, summary_json = excluded.summary_json, completed_at = excluded.completed_at",
            rusqlite::params![artifact_id, parser_version, rows_published as i64, summary.to_string(), now],
        )?;
        conn.execute(
            "UPDATE artifacts SET status = 'parsed', updated_at = ?1 WHERE artifact_id = ?2",
            rusqlite::params![now, artifact_id],
        )?;
    }

    // Delete the downloaded file to free disk space
    if let Err(e) = cleanup_artifact(&artifact_path).await {
        warn!(artifact_id, error = %e, "failed to clean up artifact file");
    }

    Ok(rows_published)
}

async fn cleanup_artifact(path: &Path) -> Result<()> {
    // Delete the file
    if path.is_file() {
        tokio::fs::remove_file(path).await?;
    }
    // Try to remove parent directory if empty (artifact_id dir)
    if let Some(parent) = path.parent() {
        let _ = tokio::fs::remove_dir(parent).await;
        // Try grandparent (collection dir) too
        if let Some(grandparent) = parent.parent() {
            let _ = tokio::fs::remove_dir(grandparent).await;
        }
    }
    Ok(())
}

// ── Streaming Parse (low memory) ──────────────────────────────────

async fn publish_structured_raw_rows(
    publisher: &ClickHousePublisher,
    registry: &SourceRegistry,
    source_id: &str,
    collection_id: &str,
    artifact: &LocalArtifact,
    result: &ingest_core::ParseResult,
) -> Result<usize> {
    publisher
        .ensure_tables(source_id, collection_id, &result.observed_schemas)
        .await?;
    let (tx, mut rx) = mpsc::channel::<RawTableRow>(64);
    let artifact2 = artifact.clone();
    let collection_id2 = collection_id.to_string();
    let registry2 = registry.clone();
    let source_id2 = source_id.to_string();
    let parse_handle = tokio::task::spawn_blocking(move || -> Result<()> {
        let mut sink = ChannelRawTableRowSink { tx };
        registry2
            .stream_structured_parse(&source_id2, &artifact2, &collection_id2, &mut sink)
            .with_context(|| format!("streaming structured raw parsed rows for {}", source_id2))
    });

    let mut batcher = ClickHouseRowBatcher::new(
        publisher,
        &result.observed_schemas,
        source_id,
        collection_id.to_string(),
        &artifact.metadata.artifact_id,
        &artifact.metadata.acquisition_uri,
    );
    while let Some(row) = rx.recv().await {
        batcher.push(row).await?;
    }
    parse_handle
        .await
        .context("joining structured raw parser task")??;
    batcher.finish().await
}

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
    rows: Vec<ingest_core::StructuredRow>,
    encoded_bytes: usize,
}

struct ClickHouseRowBatcher<'a> {
    publisher: &'a ClickHousePublisher,
    schemas_by_hash: std::collections::HashMap<String, ingest_core::ObservedSchema>,
    source_id: &'a str,
    collection_id: String,
    artifact_id: &'a str,
    remote_uri: &'a str,
    pending: std::collections::HashMap<(String, String), PendingInsertChunk>,
    published_rows: usize,
}

impl<'a> ClickHouseRowBatcher<'a> {
    fn new(
        publisher: &'a ClickHousePublisher,
        schemas: &[ingest_core::ObservedSchema],
        source_id: &'a str,
        collection_id: String,
        artifact_id: &'a str,
        remote_uri: &'a str,
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
            pending: std::collections::HashMap::new(),
            published_rows: 0,
        }
    }

    async fn push(&mut self, row: RawTableRow) -> Result<()> {
        let encoded_len = row.row.estimated_binary_size() + 96;
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
                logical_table_key: row.logical_table_key.clone(),
                schema_key: row.schema_key.clone(),
                rows: Vec::new(),
                encoded_bytes: 0,
            });
        entry.encoded_bytes += encoded_len;
        entry.rows.push(row.row);
        Ok(())
    }

    async fn finish(mut self) -> Result<usize> {
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
            .ok_or_else(|| anyhow!("missing schema for {}", chunk.schema_key))?;
        self.published_rows += self
            .publisher
            .publish_parse_result(
                self.source_id,
                &self.collection_id,
                self.artifact_id,
                self.remote_uri,
                &ingest_core::ParseResult {
                    observed_schemas: vec![schema.clone()],
                    raw_outputs: vec![ingest_core::RawTableChunk {
                        logical_table_key: chunk.logical_table_key,
                        schema_key: chunk.schema_key,
                        row_count: chunk.rows.len(),
                        rows: chunk.rows,
                    }],
                    promotions: Vec::new(),
                },
            )
            .await?;
        Ok(())
    }
}

fn sanitize_path(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-') {
                ch
            } else {
                '_'
            }
        })
        .collect()
}
