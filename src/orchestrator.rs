use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use ingest_core::{
    ArtifactMetadata, DiscoveredArtifact, DiscoveryCursorHint, LocalArtifact, ObservedSchema,
    RawTableRow, StructuredRawEvent, StructuredRawEventSink,
};
use tokio::sync::{Semaphore, mpsc};
use tracing::{error, info, warn};

use crate::clickhouse::{ClickHousePublisher, MAX_INSERT_BYTES, MAX_INSERT_ROWS, RawChunkContext};
use crate::db::{Db, now_iso};
use crate::semantic::reconcile_source_semantics;
use crate::source_registry::{ParsedArtifact, ScheduleSeed, SourceRegistry, stable_stagger_offset};

const DISCOVERY_BATCH_LIMIT: usize = 100;

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

#[derive(Debug, Clone)]
pub struct OrchestratorConfig {
    pub data_dir: PathBuf,
    pub discover_concurrency: usize,
    pub download_concurrency: usize,
    pub parse_concurrency: usize,
}

#[derive(Debug, Clone)]
struct DownloadTask {
    artifact_id: String,
    source_id: String,
    collection_id: String,
    metadata_json: String,
}

#[derive(Debug, Clone)]
struct ParseTask {
    artifact_id: String,
    source_id: String,
    collection_id: String,
    metadata_json: String,
    local_path: String,
}

pub async fn run_orchestrator(
    db: Db,
    registry: SourceRegistry,
    publisher: ClickHousePublisher,
    config: OrchestratorConfig,
    stats: Stats,
) -> Result<()> {
    // Sync schedule seeds into SQLite on startup
    let seeds = registry.schedule_seeds();
    sync_schedules(&db, &seeds).await?;

    // Recover any interrupted downloads: if file is missing, reset to discovered
    recover_interrupted(&db, &config.data_dir).await?;

    let http_client = reqwest::Client::builder()
        .user_agent("energyhistorian/0.1")
        .timeout(Duration::from_secs(120))
        .build()?;

    let discover_sem = Arc::new(Semaphore::new(config.discover_concurrency.max(1)));
    let download_sem = Arc::new(Semaphore::new(config.download_concurrency.max(1)));
    let parse_sem = Arc::new(Semaphore::new(config.parse_concurrency.max(1)));

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
        let data_dir = config.data_dir.clone();
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
    let now = Utc::now();
    for seed in seeds {
        let next_discovery_at = next_staggered_discovery_at(
            now,
            seed.poll_interval_seconds,
            seed.stagger_offset_seconds,
        );
        conn.execute(
            "INSERT INTO schedules (source_id, collection_id, enabled, poll_interval_seconds, next_discovery_at)
             VALUES (?1, ?2, 1, ?3, ?4)
             ON CONFLICT (source_id, collection_id) DO UPDATE
             SET poll_interval_seconds = excluded.poll_interval_seconds,
                 next_discovery_at = MIN(schedules.next_discovery_at, excluded.next_discovery_at)",
            rusqlite::params![
                seed.source_id,
                seed.collection_id,
                seed.poll_interval_seconds,
                next_discovery_at.to_rfc3339(),
            ],
        )?;
    }
    let count = seeds.len();
    info!(schedule_count = count, "synced schedules");
    Ok(())
}

pub(crate) async fn seed_registry_schedules(db: &Db, registry: &SourceRegistry) -> Result<()> {
    let seeds = registry.schedule_seeds();
    sync_schedules(db, &seeds).await
}

pub(crate) async fn force_schedule_due_now(
    db: &Db,
    source_id: &str,
    collection_id: &str,
) -> Result<()> {
    let conn = db.lock().await;
    conn.execute(
        "UPDATE schedules
         SET next_discovery_at = ?1
         WHERE source_id = ?2 AND collection_id = ?3",
        rusqlite::params![now_iso(), source_id, collection_id],
    )?;
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

pub(crate) async fn recover_interrupted_artifacts(db: &Db, data_dir: &Path) -> Result<()> {
    recover_interrupted(db, data_dir).await
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
                        let _ = record_schedule_failure(&db, &source_id, &collection_id, e).await;
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
        .discover(
            http_client,
            source_id,
            collection_id,
            DISCOVERY_BATCH_LIMIT,
            &cursor,
        )
        .await?;

    let now = now_iso();
    let mut inserted = 0usize;

    let conn = db.lock().await;
    for artifact in &discoveries {
        let metadata_json = serde_json::to_string(&artifact.metadata)?;
        let artifact_kind = format!("{:?}", artifact.metadata.kind);
        inserted += conn.execute(
            "INSERT INTO artifacts (artifact_id, source_id, collection_id, remote_uri, artifact_kind, metadata_json, discovered_at, publication_timestamp, status, updated_at)
             SELECT ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'discovered', ?9
             WHERE NOT EXISTS (
                 SELECT 1 FROM artifacts
                 WHERE source_id = ?2 AND collection_id = ?3 AND remote_uri = ?4
             )",
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

    // Update schedule next_discovery_at.
    // If we discovered new artifacts, reschedule quickly (10s) to continue
    // processing remaining months. Otherwise use the normal poll interval.
    let poll_interval: i64 = conn.query_row(
        "SELECT poll_interval_seconds FROM schedules WHERE source_id = ?1 AND collection_id = ?2",
        rusqlite::params![source_id, collection_id],
        |row| row.get(0),
    )?;
    let next = if inserted > 0 {
        // More work likely remains — come back soon.
        Utc::now() + chrono::Duration::seconds(10)
    } else {
        let stagger_offset =
            stable_stagger_offset(source_id, collection_id, poll_interval.max(1) as u64) as i64;
        next_staggered_discovery_at(
            Utc::now(),
            poll_interval.max(1) as u64,
            stagger_offset.max(0) as u64,
        )
    };
    conn.execute(
        "UPDATE schedules SET next_discovery_at = ?1, last_discovery_at = ?2, last_success_at = ?2, last_error_at = NULL, consecutive_failures = 0
         WHERE source_id = ?3 AND collection_id = ?4",
        rusqlite::params![next.to_rfc3339(), now, source_id, collection_id],
    )?;

    Ok(inserted)
}

pub(crate) async fn run_due_discover_batch(
    db: &Db,
    registry: &SourceRegistry,
    http_client: &reqwest::Client,
) -> Result<usize> {
    let due = find_due_schedules(db).await?;
    let mut inserted = 0usize;
    for (source_id, collection_id) in due {
        inserted += run_discover(db, registry, http_client, &source_id, &collection_id).await?;
    }
    Ok(inserted)
}

async fn fetch_cursor_hint(
    db: &Db,
    source_id: &str,
    collection_id: &str,
) -> Result<DiscoveryCursorHint> {
    let conn = db.lock().await;
    let mut stmt = conn.prepare(
        "SELECT publication_timestamp, metadata_json, remote_uri, discovered_at
         FROM artifacts
         WHERE source_id = ?1 AND collection_id = ?2",
    )?;
    let rows = stmt.query_map(rusqlite::params![source_id, collection_id], |row| {
        Ok((
            row.get::<_, Option<String>>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
        ))
    })?;

    let mut latest: Option<CursorCandidate> = None;
    let mut earliest: Option<CursorCandidate> = None;
    for row in rows {
        let (pub_ts, metadata_json, remote_uri, discovered_at) = row?;
        let metadata: ArtifactMetadata = serde_json::from_str(&metadata_json)?;
        let publication_timestamp = pub_ts
            .and_then(|value| DateTime::parse_from_rfc3339(&value).ok())
            .map(|value| value.with_timezone(&Utc));
        let discovered_at = DateTime::parse_from_rfc3339(&discovered_at)
            .map(|value| value.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now());
        let candidate = CursorCandidate {
            publication_timestamp,
            release_name: metadata
                .release_name
                .clone()
                .or_else(|| release_name_from_uri(&remote_uri)),
            discovered_at,
        };
        if latest
            .as_ref()
            .is_none_or(|current| compare_cursor_candidates(&candidate, current).is_gt())
        {
            latest = Some(candidate.clone());
        }
        if earliest
            .as_ref()
            .is_none_or(|current| compare_cursor_candidates(&candidate, current).is_lt())
        {
            earliest = Some(candidate);
        }
    }

    Ok(DiscoveryCursorHint {
        latest_publication_timestamp: latest
            .as_ref()
            .and_then(|candidate| candidate.publication_timestamp),
        latest_release_name: latest.and_then(|candidate| candidate.release_name),
        earliest_publication_timestamp: earliest
            .as_ref()
            .and_then(|candidate| candidate.publication_timestamp),
        earliest_release_name: earliest.and_then(|candidate| candidate.release_name),
    })
}

#[derive(Debug, Clone)]
struct CursorCandidate {
    publication_timestamp: Option<DateTime<Utc>>,
    release_name: Option<String>,
    discovered_at: DateTime<Utc>,
}

fn compare_cursor_candidates(
    left: &CursorCandidate,
    right: &CursorCandidate,
) -> std::cmp::Ordering {
    left.release_name
        .cmp(&right.release_name)
        .then_with(|| left.publication_timestamp.cmp(&right.publication_timestamp))
        .then_with(|| left.discovered_at.cmp(&right.discovered_at))
}

fn release_name_from_uri(remote_uri: &str) -> Option<String> {
    remote_uri.rsplit('/').next().map(str::to_string)
}

async fn record_schedule_failure(
    db: &Db,
    source_id: &str,
    collection_id: &str,
    error: &anyhow::Error,
) -> Result<()> {
    let now = Utc::now();
    let conn = db.lock().await;
    let (poll_interval, failures): (i64, i64) = conn.query_row(
        "SELECT poll_interval_seconds, consecutive_failures FROM schedules WHERE source_id = ?1 AND collection_id = ?2",
        rusqlite::params![source_id, collection_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let backoff_seconds = (30_i64 * (failures + 1)).min(poll_interval.max(30));
    let stagger_offset =
        stable_stagger_offset(source_id, collection_id, poll_interval.max(1) as u64) as i64;
    let next = next_staggered_discovery_at(
        now + chrono::Duration::seconds(backoff_seconds),
        poll_interval.max(1) as u64,
        stagger_offset.max(0) as u64,
    );
    conn.execute(
        "UPDATE schedules
         SET next_discovery_at = ?1,
             last_error_at = ?2,
             consecutive_failures = consecutive_failures + 1
         WHERE source_id = ?3 AND collection_id = ?4",
        rusqlite::params![
            next.to_rfc3339(),
            now.to_rfc3339(),
            source_id,
            collection_id
        ],
    )?;
    conn.execute(
        "UPDATE artifacts SET error_text = ?1
         WHERE source_id = ?2 AND collection_id = ?3 AND status = 'discovered'",
        rusqlite::params![format!("{error:#}"), source_id, collection_id],
    )?;
    Ok(())
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
        for task in batch {
            let permit = sem.clone().acquire_owned().await?;
            let db = db.clone();
            let registry = registry.clone();
            let http_client = http_client.clone();
            let data_dir = data_dir.clone();
            let stats = stats.clone();
            handles.push(tokio::spawn(async move {
                let result = run_download(&db, &registry, &http_client, &data_dir, &task).await;
                match &result {
                    Ok(_) => {
                        stats.downloads_ok.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        stats.downloads_err.fetch_add(1, Ordering::Relaxed);
                        warn!(artifact_id = %task.artifact_id, error = %e, "download failed");
                        // Mark as failed so we don't retry endlessly
                        let conn = db.lock().await;
                        let _ = conn.execute(
                            "UPDATE artifacts SET status = 'download_failed', error_text = ?1, updated_at = ?2 WHERE artifact_id = ?3",
                            rusqlite::params![format!("{e:#}"), now_iso(), &task.artifact_id],
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

async fn find_downloadable(db: &Db, limit: usize) -> Result<Vec<DownloadTask>> {
    let conn = db.lock().await;
    let mut stmt = conn.prepare(
        "SELECT artifact_id, source_id, collection_id, metadata_json FROM artifacts
         WHERE status = 'discovered'
         ORDER BY discovered_at ASC
         LIMIT ?1",
    )?;
    let rows = stmt
        .query_map(rusqlite::params![limit], |row| {
            Ok(DownloadTask {
                artifact_id: row.get(0)?,
                source_id: row.get(1)?,
                collection_id: row.get(2)?,
                metadata_json: row.get(3)?,
            })
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
    task: &DownloadTask,
) -> Result<()> {
    let metadata: ArtifactMetadata = serde_json::from_str(&task.metadata_json)?;
    let discovered = DiscoveredArtifact {
        metadata: metadata.clone(),
    };

    // Download to a local directory
    let artifact_dir = data_dir
        .join("artifacts")
        .join(sanitize_path(&task.source_id))
        .join(sanitize_path(&task.collection_id))
        .join(sanitize_path(&task.artifact_id));
    tokio::fs::create_dir_all(&artifact_dir).await?;

    let local = registry
        .fetch(
            http_client,
            &task.source_id,
            &task.collection_id,
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
            &task.artifact_id,
            local_path_str,
            metadata.content_sha256.as_deref().unwrap_or(""),
            file_len,
            now,
        ],
    )?;
    conn.execute(
        "UPDATE artifacts SET status = 'downloaded', updated_at = ?1 WHERE artifact_id = ?2",
        rusqlite::params![now, &task.artifact_id],
    )?;

    info!(
        artifact_id = %task.artifact_id,
        source_id = %task.source_id,
        collection_id = %task.collection_id,
        bytes = file_len,
        "downloaded"
    );
    Ok(())
}

pub(crate) async fn run_download_batch(
    db: &Db,
    registry: &SourceRegistry,
    http_client: &reqwest::Client,
    data_dir: &Path,
    limit: usize,
) -> Result<usize> {
    let batch = find_downloadable(db, limit).await?;
    let count = batch.len();
    for task in batch {
        run_download(db, registry, http_client, data_dir, &task).await?;
    }
    Ok(count)
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
        for task in batch {
            let permit = sem.clone().acquire_owned().await?;
            let db = db.clone();
            let registry = registry.clone();
            let publisher = publisher.clone();
            let stats = stats.clone();
            handles.push(tokio::spawn(async move {
                let result = run_parse(&db, &registry, &publisher, &task).await;
                match &result {
                    Ok(rows) => {
                        stats.parses_ok.fetch_add(1, Ordering::Relaxed);
                        info!(
                            artifact_id = %task.artifact_id,
                            source_id = %task.source_id,
                            collection_id = %task.collection_id,
                            rows,
                            "parsed"
                        );
                    }
                    Err(e) => {
                        stats.parses_err.fetch_add(1, Ordering::Relaxed);
                        warn!(artifact_id = %task.artifact_id, error = %e, "parse failed");
                        let conn = db.lock().await;
                        let parser_version = registry
                            .parser_version(&task.source_id)
                            .unwrap_or_else(|_| "unknown".to_string());
                        let now = now_iso();
                        let _ = conn.execute(
                            "INSERT INTO parse_runs (artifact_id, parser_version, status, row_count, summary_json, started_at, completed_at, error_text)
                             VALUES (?1, ?2, 'failed', NULL, NULL, ?3, ?3, ?4)
                             ON CONFLICT (artifact_id, parser_version) DO UPDATE
                             SET status = 'failed', row_count = NULL, summary_json = NULL, completed_at = excluded.completed_at, error_text = excluded.error_text",
                            rusqlite::params![&task.artifact_id, parser_version, now, format!("{e:#}")],
                        );
                        let _ = conn.execute(
                            "UPDATE artifacts SET status = 'parse_failed', error_text = ?1, updated_at = ?2 WHERE artifact_id = ?3",
                            rusqlite::params![format!("{e:#}"), now, &task.artifact_id],
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

async fn find_parseable(db: &Db, limit: usize) -> Result<Vec<ParseTask>> {
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
            Ok(ParseTask {
                artifact_id: row.get(0)?,
                source_id: row.get(1)?,
                collection_id: row.get(2)?,
                metadata_json: row.get(3)?,
                local_path: row.get(4)?,
            })
        })?
        .filter_map(|r| r.ok())
        .collect();
    Ok(rows)
}

async fn run_parse(
    db: &Db,
    registry: &SourceRegistry,
    publisher: &ClickHousePublisher,
    task: &ParseTask,
) -> Result<usize> {
    let metadata: ArtifactMetadata = serde_json::from_str(&task.metadata_json)?;
    let parser_version = registry.parser_version(&task.source_id)?;

    // Check if already parsed with this version
    {
        let conn = db.lock().await;
        let already: bool = conn
            .query_row(
                "SELECT COUNT(*) FROM parse_runs WHERE artifact_id = ?1 AND parser_version = ?2 AND status = 'succeeded'",
                rusqlite::params![&task.artifact_id, parser_version],
                |row| Ok(row.get::<_, i64>(0)? > 0),
            )
            .unwrap_or(false);
        if already {
            conn.execute(
                "UPDATE artifacts SET status = 'parsed', updated_at = ?1 WHERE artifact_id = ?2",
                rusqlite::params![now_iso(), &task.artifact_id],
            )?;
            return Ok(0);
        }
    }

    let artifact_path = PathBuf::from(&task.local_path);
    let artifact = LocalArtifact {
        metadata,
        local_path: artifact_path.clone(),
    };

    let parsed = registry.parse(&task.source_id, &task.collection_id, artifact.clone())?;
    let (rows_published, semantic_jobs) = match parsed {
        ParsedArtifact::StructuredRaw {
            artifact: ref local_artifact,
        } => {
            let publish = publish_structured_raw_rows(
                publisher,
                registry,
                &task.source_id,
                &task.collection_id,
                local_artifact,
            )
            .await?;
            let semantic_jobs = if publish.new_schemas_observed {
                reconcile_source_semantics(publisher, registry, &task.source_id).await?
            } else {
                0
            };
            (publish.rows_published, semantic_jobs)
        }
        ParsedArtifact::RawMetadata {
            artifact: ref local_artifact,
            ref result,
        } => {
            let rows = publisher
                .publish_raw_plugin_parse_result(
                    &task.source_id,
                    &task.collection_id,
                    local_artifact,
                    result,
                )
                .await?;
            let semantic_jobs =
                reconcile_source_semantics(publisher, registry, &task.source_id).await?;
            (rows, semantic_jobs)
        }
    };

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
            rusqlite::params![&task.artifact_id, parser_version, rows_published as i64, summary.to_string(), now],
        )?;
        conn.execute(
            "UPDATE artifacts SET status = 'parsed', updated_at = ?1 WHERE artifact_id = ?2",
            rusqlite::params![now, &task.artifact_id],
        )?;
    }

    // Delete the downloaded file to free disk space
    if let Err(e) = cleanup_artifact(&artifact_path).await {
        warn!(artifact_id = %task.artifact_id, error = %e, "failed to clean up artifact file");
    }

    Ok(rows_published)
}

pub(crate) async fn run_parse_batch(
    db: &Db,
    registry: &SourceRegistry,
    publisher: &ClickHousePublisher,
    limit: usize,
) -> Result<usize> {
    let batch = find_parseable(db, limit).await?;
    let mut rows = 0usize;
    for task in batch {
        rows += run_parse(db, registry, publisher, &task).await?;
    }
    Ok(rows)
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
) -> Result<StructuredPublishOutcome> {
    let (tx, mut rx) = mpsc::channel::<StructuredRawEvent>(64);
    let artifact2 = artifact.clone();
    let collection_id2 = collection_id.to_string();
    let registry2 = registry.clone();
    let source_id2 = source_id.to_string();
    let parse_handle = tokio::task::spawn_blocking(move || -> Result<()> {
        let mut sink = ChannelStructuredRawEventSink { tx };
        registry2
            .stream_structured_parse_events(&source_id2, &artifact2, &collection_id2, &mut sink)
            .with_context(|| format!("streaming structured raw parsed rows for {}", source_id2))
    });

    let mut batcher = ClickHouseRowBatcher::new(
        publisher,
        source_id,
        collection_id.to_string(),
        &artifact.metadata.artifact_id,
        &artifact.metadata.acquisition_uri,
    );
    while let Some(event) = rx.recv().await {
        match event {
            StructuredRawEvent::Schema(schema) => {
                batcher.register_schema(schema).await?;
            }
            StructuredRawEvent::Row(row) => {
                batcher.push(row).await?;
            }
        }
    }
    parse_handle
        .await
        .context("joining structured raw parser task")??;
    batcher.finish().await
}

struct ChannelStructuredRawEventSink {
    tx: mpsc::Sender<StructuredRawEvent>,
}

impl StructuredRawEventSink for ChannelStructuredRawEventSink {
    fn accept(&mut self, event: StructuredRawEvent) -> Result<()> {
        self.tx
            .blocking_send(event)
            .map_err(|_| anyhow!("structured raw stream channel closed while parsing"))
    }
}

struct StructuredPublishOutcome {
    rows_published: usize,
    new_schemas_observed: bool,
}

struct PendingInsertChunk {
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
    new_schemas_observed: bool,
}

impl<'a> ClickHouseRowBatcher<'a> {
    fn new(
        publisher: &'a ClickHousePublisher,
        source_id: &'a str,
        collection_id: String,
        artifact_id: &'a str,
        remote_uri: &'a str,
    ) -> Self {
        Self {
            publisher,
            schemas_by_hash: std::collections::HashMap::new(),
            source_id,
            collection_id,
            artifact_id,
            remote_uri,
            pending: std::collections::HashMap::new(),
            published_rows: 0,
            new_schemas_observed: false,
        }
    }

    async fn register_schema(&mut self, schema: ObservedSchema) -> Result<()> {
        let schema_hash = schema.schema_key.header_hash.clone();
        if self.schemas_by_hash.contains_key(&schema_hash) {
            return Ok(());
        }
        if self
            .publisher
            .ensure_table(self.source_id, &self.collection_id, &schema)
            .await?
        {
            self.new_schemas_observed = true;
        }
        self.schemas_by_hash.insert(schema_hash, schema);
        Ok(())
    }

    async fn push(&mut self, row: RawTableRow) -> Result<()> {
        if !self.schemas_by_hash.contains_key(&row.schema_key) {
            return Err(anyhow!(
                "row arrived before schema registration for {}",
                row.schema_key
            ));
        }
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
                schema_key: row.schema_key.clone(),
                rows: Vec::new(),
                encoded_bytes: 0,
            });
        entry.encoded_bytes += encoded_len;
        entry.rows.push(row.row);
        Ok(())
    }

    async fn finish(mut self) -> Result<StructuredPublishOutcome> {
        let keys = self.pending.keys().cloned().collect::<Vec<_>>();
        for key in keys {
            self.flush_key(&key).await?;
        }
        Ok(StructuredPublishOutcome {
            rows_published: self.published_rows,
            new_schemas_observed: self.new_schemas_observed,
        })
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
            .publish_prepared_raw_chunk(
                RawChunkContext {
                    schema,
                    source_id: self.source_id,
                    collection_id: &self.collection_id,
                    artifact_id: self.artifact_id,
                    remote_uri: self.remote_uri,
                    processed_at: Utc::now(),
                },
                &chunk.rows,
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

fn next_staggered_discovery_at(
    now: DateTime<Utc>,
    poll_interval_seconds: u64,
    stagger_offset_seconds: u64,
) -> DateTime<Utc> {
    let interval = poll_interval_seconds.max(1) as i64;
    let offset = (stagger_offset_seconds % poll_interval_seconds.max(1)) as i64;
    let now_ts = now.timestamp();
    let base = now_ts - (now_ts.rem_euclid(interval));
    let mut next_ts = base + offset;
    if next_ts <= now_ts {
        next_ts += interval;
    }
    DateTime::<Utc>::from_timestamp(next_ts, 0).unwrap_or(now + chrono::Duration::seconds(interval))
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use anyhow::{Result, bail};
    use chrono::Utc;
    use ingest_core::{
        ArtifactKind, ArtifactMetadata, BoxedFuture, CollectionCompletion, CompletionUnit,
        DiscoveredArtifact, DiscoveryCursorHint, DiscoveryRequest, LocalArtifact, ParseResult,
        PluginCapabilities, PromotionSpec, RawTableRowSink, RunContext, RuntimePluginParseResult,
        RuntimeSourcePlugin, SourceCollection, SourceDescriptor, SourceMetadataDocument,
        SourcePlugin, StructuredRawEventSink, TaskBlueprint, TaskKind,
    };
    use tempfile::TempDir;

    use super::*;
    use crate::db::{Db, open_database};
    use crate::source_registry::SourceRegistry;

    #[derive(Clone)]
    struct FakeDiscoverPlugin {
        source_id: &'static str,
        collection_id: &'static str,
        artifacts: Vec<DiscoveredArtifact>,
    }

    impl SourcePlugin for FakeDiscoverPlugin {
        fn descriptor(&self) -> SourceDescriptor {
            SourceDescriptor {
                source_id: self.source_id.to_string(),
                domain: "test".to_string(),
                description: "test plugin".to_string(),
                versioned_metadata: true,
                historical_backfill_supported: true,
            }
        }

        fn capabilities(&self) -> PluginCapabilities {
            PluginCapabilities {
                supports_backfill: true,
                supports_schema_registry: false,
                supports_historical_media: false,
                notes: Vec::new(),
            }
        }

        fn collections(&self) -> Vec<SourceCollection> {
            vec![SourceCollection {
                id: self.collection_id.to_string(),
                display_name: "Test".to_string(),
                description: "Test collection".to_string(),
                retrieval_modes: vec!["discover".to_string()],
                completion: CollectionCompletion {
                    unit: CompletionUnit::Artifact,
                    dedupe_keys: vec!["artifact_id".to_string()],
                    cursor_field: Some("published_at".to_string()),
                    mutable_window_seconds: Some(60),
                    notes: Vec::new(),
                },
                task_blueprints: vec![TaskBlueprint {
                    kind: TaskKind::Discover,
                    description: "discover".to_string(),
                    max_concurrency: 1,
                    queue: "discover".to_string(),
                    idempotency_scope: "artifact".to_string(),
                }],
                default_poll_interval_seconds: Some(300),
            }]
        }

        fn metadata_catalog(&self) -> Vec<SourceMetadataDocument> {
            Vec::new()
        }

        fn discover(
            &self,
            _request: &DiscoveryRequest,
            _ctx: &RunContext,
        ) -> Result<Vec<DiscoveredArtifact>> {
            bail!("unused")
        }

        fn fetch(
            &self,
            _artifact: &DiscoveredArtifact,
            _ctx: &RunContext,
        ) -> Result<LocalArtifact> {
            bail!("unused")
        }

        fn inspect_parse(
            &self,
            _artifact: &LocalArtifact,
            _ctx: &RunContext,
        ) -> Result<ParseResult> {
            bail!("unused")
        }

        fn stream_parse(
            &self,
            _artifact: &LocalArtifact,
            _ctx: &RunContext,
            _sink: &mut dyn RawTableRowSink,
        ) -> Result<()> {
            bail!("unused")
        }

        fn promotion_plan(&self) -> &'static [PromotionSpec] {
            &[]
        }
    }

    impl RuntimeSourcePlugin for FakeDiscoverPlugin {
        fn parser_version(&self) -> &'static str {
            "fake/0.1"
        }

        fn discover_collection_async<'a>(
            &'a self,
            _client: &'a reqwest::Client,
            collection_id: &'a str,
            _limit: usize,
            _cursor: &'a DiscoveryCursorHint,
            _ctx: &'a RunContext,
        ) -> BoxedFuture<'a, Result<Vec<DiscoveredArtifact>>> {
            Box::pin(async move {
                if collection_id != self.collection_id {
                    bail!("unexpected collection");
                }
                Ok(self.artifacts.clone())
            })
        }

        fn fetch_artifact_async<'a>(
            &'a self,
            _client: &'a reqwest::Client,
            _collection_id: &'a str,
            _artifact: &'a DiscoveredArtifact,
            _output_dir: &'a Path,
        ) -> BoxedFuture<'a, Result<LocalArtifact>> {
            Box::pin(async move { bail!("unused") })
        }

        fn parse_artifact_runtime(
            &self,
            _collection_id: &str,
            _artifact: LocalArtifact,
            _ctx: &RunContext,
        ) -> Result<RuntimePluginParseResult> {
            bail!("unused")
        }

        fn stream_structured_parse_runtime(
            &self,
            _artifact: &LocalArtifact,
            _collection_id: &str,
            _ctx: &RunContext,
            _sink: &mut dyn RawTableRowSink,
        ) -> Result<()> {
            bail!("unused")
        }

        fn stream_structured_parse_events_runtime(
            &self,
            _artifact: &LocalArtifact,
            _collection_id: &str,
            _ctx: &RunContext,
            _sink: &mut dyn StructuredRawEventSink,
        ) -> Result<()> {
            bail!("unused")
        }
    }

    fn artifact(id: &str, source_id: &str) -> DiscoveredArtifact {
        DiscoveredArtifact {
            metadata: ArtifactMetadata {
                artifact_id: id.to_string(),
                source_id: source_id.to_string(),
                acquisition_uri: format!("https://example.com/{id}.zip"),
                discovered_at: Utc::now(),
                fetched_at: None,
                published_at: Some(Utc::now()),
                content_sha256: None,
                content_length_bytes: None,
                kind: ArtifactKind::ZipArchive,
                parser_version: "fake/0.1".to_string(),
                model_version: None,
                release_name: Some("2024-01".to_string()),
            },
        }
    }

    fn temp_db() -> Result<(TempDir, Db)> {
        let dir = tempfile::tempdir()?;
        let db = open_database(&dir.path().join("historian.db"))?;
        Ok((dir, db))
    }

    async fn insert_artifact_row(
        db: &Db,
        artifact_id: &str,
        source_id: &str,
        collection_id: &str,
        status: &str,
    ) -> Result<()> {
        let metadata = ArtifactMetadata {
            artifact_id: artifact_id.to_string(),
            source_id: source_id.to_string(),
            acquisition_uri: format!("https://example.com/{artifact_id}.zip"),
            discovered_at: Utc::now(),
            fetched_at: None,
            published_at: Some(Utc::now()),
            content_sha256: None,
            content_length_bytes: None,
            kind: ArtifactKind::ZipArchive,
            parser_version: "fake/0.1".to_string(),
            model_version: None,
            release_name: Some("2024-01".to_string()),
        };
        let conn = db.lock().await;
        conn.execute(
            "INSERT INTO artifacts (
                artifact_id, source_id, collection_id, remote_uri, artifact_kind,
                metadata_json, discovered_at, publication_timestamp, status, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            rusqlite::params![
                artifact_id,
                source_id,
                collection_id,
                metadata.acquisition_uri,
                "ZipArchive",
                serde_json::to_string(&metadata)?,
                Utc::now().to_rfc3339(),
                metadata.published_at.map(|ts| ts.to_rfc3339()),
                status,
                Utc::now().to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    #[tokio::test]
    async fn run_due_discover_batch_inserts_artifacts_and_reschedules_quickly() -> Result<()> {
        let (_dir, db) = temp_db()?;
        let source_id = "test.source";
        let collection_id = "test-collection";
        let registry = SourceRegistry::from_plugin_for_test(FakeDiscoverPlugin {
            source_id,
            collection_id,
            artifacts: vec![
                artifact("artifact-one", source_id),
                artifact("artifact-two", source_id),
            ],
        });
        let client = reqwest::Client::new();

        seed_registry_schedules(&db, &registry).await?;
        force_schedule_due_now(&db, source_id, collection_id).await?;
        let before = Utc::now();

        let inserted = run_due_discover_batch(&db, &registry, &client).await?;

        assert_eq!(inserted, 2);
        let conn = db.lock().await;
        let artifact_count: i64 =
            conn.query_row("SELECT count(*) FROM artifacts", [], |row| row.get(0))?;
        assert_eq!(artifact_count, 2);
        let next_discovery_at: String = conn.query_row(
            "SELECT next_discovery_at FROM schedules WHERE source_id = ?1 AND collection_id = ?2",
            rusqlite::params![source_id, collection_id],
            |row| row.get(0),
        )?;
        drop(conn);

        let next_discovery_at =
            chrono::DateTime::parse_from_rfc3339(&next_discovery_at)?.with_timezone(&Utc);
        assert!(next_discovery_at >= before + chrono::Duration::seconds(9));
        assert!(next_discovery_at <= before + chrono::Duration::seconds(15));
        Ok(())
    }

    #[tokio::test]
    async fn find_downloadable_respects_limit() -> Result<()> {
        let (_dir, db) = temp_db()?;
        for idx in 0..12 {
            insert_artifact_row(
                &db,
                &format!("artifact-{idx}"),
                "test.source",
                "test-collection",
                "discovered",
            )
            .await?;
        }

        let tasks = find_downloadable(&db, 5).await?;

        assert_eq!(tasks.len(), 5);
        Ok(())
    }

    #[tokio::test]
    async fn find_parseable_respects_limit_and_requires_download_rows() -> Result<()> {
        let (_dir, db) = temp_db()?;
        for idx in 0..8 {
            let artifact_id = format!("artifact-{idx}");
            insert_artifact_row(
                &db,
                &artifact_id,
                "test.source",
                "test-collection",
                "downloaded",
            )
            .await?;
            let conn = db.lock().await;
            conn.execute(
                "INSERT INTO downloads (artifact_id, local_path, content_sha256, content_length_bytes, downloaded_at)
                 VALUES (?1, ?2, '', 0, ?3)",
                rusqlite::params![
                    artifact_id,
                    PathBuf::from(format!("/tmp/{artifact_id}.zip")).display().to_string(),
                    Utc::now().to_rfc3339()
                ],
            )?;
        }

        let tasks = find_parseable(&db, 3).await?;

        assert_eq!(tasks.len(), 3);
        Ok(())
    }
}
