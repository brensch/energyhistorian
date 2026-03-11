use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, TimeDelta, Utc};
use futures_util::future::poll_fn;
use ingest_core::DiscoveredArtifact;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_postgres::{AsyncMessage, Client, NoTls, Row};

#[derive(Debug, Clone)]
pub struct CollectionScheduleSeed {
    pub source_id: String,
    pub collection_id: String,
    pub poll_interval_seconds: u64,
    pub stagger_offset_seconds: u64,
    pub scheduler_enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRecord {
    pub task_id: String,
    pub source_id: String,
    pub collection_id: String,
    pub task_kind: String,
    pub artifact_id: Option<String>,
    pub task_state: String,
    pub payload_json: Value,
    pub attempts: i32,
    pub max_attempts: i32,
}

#[derive(Debug)]
pub enum TaskTrigger {
    Notification,
    Poll,
}

#[derive(Debug, Clone)]
pub struct StoredArtifactRow {
    pub bucket: String,
    pub key: String,
    pub content_sha256: String,
    pub content_length_bytes: i64,
}

pub async fn connect_listener(postgres_url: &str) -> Result<mpsc::UnboundedReceiver<TaskTrigger>> {
    let (client, mut connection) = tokio_postgres::connect(postgres_url, NoTls)
        .await
        .with_context(|| format!("connecting listener to postgres at {postgres_url}"))?;
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        loop {
            let message = poll_fn(|cx| connection.poll_message(cx)).await;
            match message {
                Some(Ok(AsyncMessage::Notification(_))) => {
                    let _ = tx.send(TaskTrigger::Notification);
                }
                Some(Ok(_)) => {}
                Some(Err(error)) => {
                    tracing::warn!(error = ?error, "task notification listener stopped");
                    break;
                }
                None => break,
            }
        }
    });
    client.batch_execute("LISTEN task_events").await?;
    Ok(rx)
}

pub async fn wait_for_trigger(
    rx: &mut mpsc::UnboundedReceiver<TaskTrigger>,
    poll_interval: Duration,
) -> TaskTrigger {
    tokio::select! {
        Some(trigger) = rx.recv() => trigger,
        _ = tokio::time::sleep(poll_interval) => TaskTrigger::Poll,
    }
}

pub async fn try_acquire_scheduler_leadership(client: &Client) -> Result<bool> {
    let row = client
        .query_one("SELECT pg_try_advisory_lock(84233701)", &[])
        .await?;
    Ok(row.get(0))
}

pub async fn sync_collection_schedules(
    client: &Client,
    seeds: &[CollectionScheduleSeed],
) -> Result<()> {
    let now = Utc::now();
    for seed in seeds {
        let next_discovery_at = now;
        client.execute(
            "INSERT INTO collection_schedules \
             (source_id, collection_id, scheduler_enabled, poll_interval_seconds, next_discovery_at, updated_at) \
             VALUES ($1, $2, $3, $4, $5, $5) \
             ON CONFLICT (source_id, collection_id) DO UPDATE \
             SET scheduler_enabled = EXCLUDED.scheduler_enabled, \
                 poll_interval_seconds = EXCLUDED.poll_interval_seconds, \
                 updated_at = EXCLUDED.updated_at",
            &[
                &seed.source_id,
                &seed.collection_id,
                &seed.scheduler_enabled,
                &(seed.poll_interval_seconds as i32),
                &next_discovery_at,
            ],
        ).await?;
    }
    Ok(())
}

pub async fn enqueue_due_discover_tasks(
    client: &Client,
    seeds: &[CollectionScheduleSeed],
) -> Result<usize> {
    let mut inserted = 0usize;
    let now = Utc::now();
    for seed in seeds.iter().filter(|seed| seed.scheduler_enabled) {
        let row = client.query_opt(
            "SELECT next_discovery_at FROM collection_schedules WHERE source_id = $1 AND collection_id = $2",
            &[&seed.source_id, &seed.collection_id],
        ).await?;
        let Some(row) = row else { continue };
        let next_discovery_at: DateTime<Utc> = row.get(0);
        if next_discovery_at > now {
            continue;
        }
        let inflight: i64 = client.query_one(
            "SELECT COUNT(*) FROM task_queue WHERE source_id = $1 AND collection_id = $2 AND task_kind = 'discover' AND task_state IN ('queued','running')",
            &[&seed.source_id, &seed.collection_id],
        ).await?.get(0);
        if inflight > 0 {
            continue;
        }
        let task_id = format!(
            "{}:{}:discover:{}",
            seed.source_id,
            seed.collection_id,
            next_discovery_at.timestamp()
        );
        let idempotency = format!(
            "{}:{}:{}",
            seed.source_id,
            seed.collection_id,
            next_discovery_at.timestamp()
        );
        let payload = json!({ "limit": 10 });
        let changed = client.execute(
            "INSERT INTO task_queue \
             (task_id, source_id, collection_id, task_kind, artifact_id, task_state, idempotency_key, payload_json, priority, available_at, created_at, updated_at) \
             VALUES ($1, $2, $3, 'discover', NULL, 'queued', $4, $5, 10, $6, $6, $6) \
             ON CONFLICT DO NOTHING",
            &[&task_id, &seed.source_id, &seed.collection_id, &idempotency, &payload, &now],
        ).await?;
        if changed > 0 {
            inserted += 1;
            let next_time = next_discovery_time(seed, now);
            client.execute(
                "UPDATE collection_schedules SET next_discovery_at = $1, updated_at = $2 WHERE source_id = $3 AND collection_id = $4",
                &[&next_time, &now, &seed.source_id, &seed.collection_id],
            ).await?;
        }
    }
    if inserted > 0 {
        notify_task_event(client).await?;
    }
    Ok(inserted)
}

pub async fn claim_tasks(
    client: &mut Client,
    task_kind: &str,
    worker_id: &str,
    limit: i64,
    lease_duration: Duration,
) -> Result<Vec<TaskRecord>> {
    let lease_expires_at =
        Utc::now() + TimeDelta::seconds(i64::try_from(lease_duration.as_secs()).unwrap_or(300));
    let transaction = client.transaction().await?;
    let rows = transaction
        .query(
            "WITH claimed AS ( \
               SELECT task_id \
               FROM task_queue \
               WHERE task_kind = $1 \
                 AND task_state = 'queued' \
                 AND available_at <= NOW() \
                 AND (lease_expires_at IS NULL OR lease_expires_at < NOW()) \
               ORDER BY priority ASC, available_at ASC, created_at ASC \
               FOR UPDATE SKIP LOCKED \
               LIMIT $2 \
             ) \
             UPDATE task_queue t \
             SET task_state = 'running', \
                 lease_owner = $3, \
                 lease_expires_at = $4, \
                 attempts = attempts + 1, \
                 started_at = COALESCE(started_at, NOW()), \
                 updated_at = NOW() \
             FROM claimed \
             WHERE t.task_id = claimed.task_id \
             RETURNING t.task_id, t.source_id, t.collection_id, t.task_kind, t.artifact_id, t.task_state, t.payload_json, t.attempts, t.max_attempts",
            &[&task_kind, &limit, &worker_id, &lease_expires_at],
        )
        .await?;
    transaction.commit().await?;
    rows.into_iter().map(task_from_row).collect()
}

pub async fn requeue_expired_leases(client: &Client) -> Result<usize> {
    let updated = client
        .execute(
            "UPDATE task_queue \
             SET task_state = 'queued', available_at = NOW(), lease_owner = NULL, lease_expires_at = NULL, updated_at = NOW() \
             WHERE task_state = 'running' AND lease_expires_at IS NOT NULL AND lease_expires_at < NOW()",
            &[],
        )
        .await?;
    if updated > 0 {
        notify_task_event(client).await?;
    }
    Ok(updated as usize)
}

pub async fn complete_task(client: &Client, task_id: &str) -> Result<()> {
    client.execute(
        "UPDATE task_queue SET task_state = 'succeeded', completed_at = NOW(), lease_owner = NULL, lease_expires_at = NULL, updated_at = NOW() WHERE task_id = $1",
        &[&task_id],
    ).await?;
    Ok(())
}

pub async fn renew_task_lease(
    client: &Client,
    task_id: &str,
    worker_id: &str,
    lease_duration: Duration,
) -> Result<bool> {
    let lease_expires_at = Utc::now()
        + TimeDelta::seconds(i64::try_from(lease_duration.as_secs()).unwrap_or(300));
    let updated = client
        .execute(
            "UPDATE task_queue \
             SET lease_expires_at = $3, updated_at = NOW() \
             WHERE task_id = $1 AND task_state = 'running' AND lease_owner = $2",
            &[&task_id, &worker_id, &lease_expires_at],
        )
        .await?;
    Ok(updated > 0)
}

pub async fn fail_task(client: &Client, task: &TaskRecord, error_text: &str) -> Result<()> {
    let next_attempt = task.attempts + 1;
    if next_attempt >= task.max_attempts {
        client.execute(
            "UPDATE task_queue SET task_state = 'dead', last_error = $2, lease_owner = NULL, lease_expires_at = NULL, updated_at = NOW() WHERE task_id = $1",
            &[&task.task_id, &error_text],
        ).await?;
        return Ok(());
    }
    let delay = retry_delay_for_attempt(next_attempt);
    let available_at = Utc::now() + TimeDelta::seconds(delay.num_seconds());
    client.execute(
        "UPDATE task_queue SET task_state = 'queued', available_at = $2, last_error = $3, lease_owner = NULL, lease_expires_at = NULL, updated_at = NOW() WHERE task_id = $1",
        &[&task.task_id, &available_at, &error_text],
    ).await?;
    notify_task_event(client).await?;
    Ok(())
}

pub async fn fetch_discovered_artifact(
    client: &Client,
    artifact_id: &str,
) -> Result<DiscoveredArtifact> {
    let row = client
        .query_one(
            "SELECT artifact_metadata_json FROM discovered_artifacts WHERE artifact_id = $1",
            &[&artifact_id],
        )
        .await?;
    let metadata: ingest_core::ArtifactMetadata = serde_json::from_value(row.get(0))?;
    Ok(DiscoveredArtifact { metadata })
}

pub async fn fetch_stored_artifact(
    client: &Client,
    artifact_id: &str,
) -> Result<StoredArtifactRow> {
    let row = client.query_one(
        "SELECT object_store_bucket, object_store_key, content_sha256, content_length_bytes FROM stored_artifacts WHERE artifact_id = $1",
        &[&artifact_id],
    ).await?;
    Ok(StoredArtifactRow {
        bucket: row.get(0),
        key: row.get(1),
        content_sha256: row.get(2),
        content_length_bytes: row.get(3),
    })
}

pub async fn parse_already_succeeded(
    client: &Client,
    artifact_id: &str,
    parser_version: &str,
) -> Result<bool> {
    let row = client.query_one(
        "SELECT COUNT(*) FROM parse_runs WHERE artifact_id = $1 AND parser_version = $2 AND status = 'succeeded'",
        &[&artifact_id, &parser_version],
    ).await?;
    Ok(row.get::<_, i64>(0) > 0)
}

pub async fn record_discoveries(
    client: &Client,
    source_id: &str,
    collection_id: &str,
    discoveries: &[DiscoveredArtifact],
) -> Result<usize> {
    let mut fetch_tasks = 0usize;
    for artifact in discoveries {
        let metadata = serde_json::to_value(&artifact.metadata)?;
        let artifact_kind = format!("{:?}", artifact.metadata.kind);
        let idempotency_key = format!("{}:{}", source_id, artifact.metadata.artifact_id);
        client.execute(
            "INSERT INTO discovered_artifacts \
             (artifact_id, source_id, collection_id, remote_uri, artifact_kind, idempotency_key, discovered_at, publication_timestamp, artifact_metadata_json, status) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'discovered') \
             ON CONFLICT (artifact_id) DO UPDATE \
             SET remote_uri = EXCLUDED.remote_uri, artifact_metadata_json = EXCLUDED.artifact_metadata_json, status = 'discovered'",
            &[
                &artifact.metadata.artifact_id,
                &source_id,
                &collection_id,
                &artifact.metadata.acquisition_uri,
                &artifact_kind,
                &idempotency_key,
                &artifact.metadata.discovered_at,
                &artifact.metadata.published_at,
                &metadata,
            ],
        ).await?;
        let fetch_task_id = format!(
            "{}:{}:fetch:{}",
            source_id, collection_id, artifact.metadata.artifact_id
        );
        let payload = json!({ "artifact_id": artifact.metadata.artifact_id });
        let changed = client.execute(
            "INSERT INTO task_queue \
             (task_id, source_id, collection_id, task_kind, artifact_id, task_state, idempotency_key, payload_json, priority, available_at, created_at, updated_at) \
             VALUES ($1, $2, $3, 'fetch', $4, 'queued', $5, $6, 20, NOW(), NOW(), NOW()) \
             ON CONFLICT DO NOTHING",
            &[
                &fetch_task_id,
                &source_id,
                &collection_id,
                &artifact.metadata.artifact_id,
                &artifact.metadata.artifact_id,
                &payload,
            ],
        ).await?;
        fetch_tasks += changed as usize;
    }
    if fetch_tasks > 0 {
        notify_task_event(client).await?;
    }
    Ok(fetch_tasks)
}

pub async fn record_fetch_success(
    client: &Client,
    task: &TaskRecord,
    object_bucket: &str,
    object_key: &str,
    sha256: &str,
    content_length_bytes: i64,
    etag: Option<&str>,
    parser_version: &str,
) -> Result<()> {
    let artifact_id = task
        .artifact_id
        .as_deref()
        .ok_or_else(|| anyhow!("fetch task missing artifact_id"))?;
    client.execute(
        "INSERT INTO stored_artifacts \
         (artifact_id, object_store_bucket, object_store_key, content_sha256, content_length_bytes, etag, stored_at) \
         VALUES ($1, $2, $3, $4, $5, $6, NOW()) \
         ON CONFLICT (artifact_id) DO UPDATE \
         SET object_store_bucket = EXCLUDED.object_store_bucket, \
             object_store_key = EXCLUDED.object_store_key, \
             content_sha256 = EXCLUDED.content_sha256, \
             content_length_bytes = EXCLUDED.content_length_bytes, \
             etag = EXCLUDED.etag, \
             stored_at = NOW()",
        &[
            &artifact_id,
            &object_bucket,
            &object_key,
            &sha256,
            &content_length_bytes,
            &etag,
        ],
    ).await?;
    client
        .execute(
            "UPDATE discovered_artifacts SET status = 'stored' WHERE artifact_id = $1",
            &[&artifact_id],
        )
        .await?;
    let parse_task_id = format!(
        "{}:{}:parse:{}",
        task.source_id, task.collection_id, artifact_id
    );
    let payload = json!({ "artifact_id": artifact_id, "parser_version": parser_version });
    client.execute(
        "INSERT INTO task_queue \
         (task_id, source_id, collection_id, task_kind, artifact_id, task_state, idempotency_key, payload_json, priority, available_at, created_at, updated_at) \
         VALUES ($1, $2, $3, 'parse', $4, 'queued', $5, $6, 30, NOW(), NOW(), NOW()) \
         ON CONFLICT DO NOTHING",
        &[
            &parse_task_id,
            &task.source_id,
            &task.collection_id,
            &artifact_id,
            &format!("{artifact_id}:{parser_version}"),
            &payload,
        ],
    ).await?;
    notify_task_event(client).await?;
    Ok(())
}

pub async fn mark_discover_success(
    client: &Client,
    source_id: &str,
    collection_id: &str,
) -> Result<()> {
    client.execute(
        "UPDATE collection_schedules \
         SET last_discovery_at = NOW(), last_success_at = NOW(), consecutive_failures = 0, updated_at = NOW() \
         WHERE source_id = $1 AND collection_id = $2",
        &[&source_id, &collection_id],
    ).await?;
    Ok(())
}

pub async fn mark_discover_failure(
    client: &Client,
    source_id: &str,
    collection_id: &str,
) -> Result<()> {
    client.execute(
        "UPDATE collection_schedules \
         SET last_discovery_at = NOW(), last_error_at = NOW(), consecutive_failures = consecutive_failures + 1, updated_at = NOW() \
         WHERE source_id = $1 AND collection_id = $2",
        &[&source_id, &collection_id],
    ).await?;
    Ok(())
}

pub async fn record_parse_success(
    client: &Client,
    task: &TaskRecord,
    parser_service: &str,
    parser_version: &str,
    row_count: i64,
    summary: Value,
) -> Result<()> {
    let artifact_id = task
        .artifact_id
        .as_deref()
        .ok_or_else(|| anyhow!("parse task missing artifact_id"))?;
    let run_id = format!("{artifact_id}:{parser_version}");
    client.execute(
        "INSERT INTO parse_runs \
         (run_id, artifact_id, parser_service, parser_version, status, row_count, output_summary_json, started_at, completed_at) \
         VALUES ($1, $2, $3, $4, 'succeeded', $5, $6, NOW(), NOW()) \
         ON CONFLICT (artifact_id, parser_version) DO UPDATE \
         SET status = 'succeeded', row_count = EXCLUDED.row_count, output_summary_json = EXCLUDED.output_summary_json, completed_at = NOW()",
        &[&run_id, &artifact_id, &parser_service, &parser_version, &row_count, &summary],
    ).await?;
    client
        .execute(
            "UPDATE discovered_artifacts SET status = 'parsed' WHERE artifact_id = $1",
            &[&artifact_id],
        )
        .await?;
    Ok(())
}

pub fn stable_stagger_offset_seconds(
    source_id: &str,
    collection_id: &str,
    poll_interval_seconds: u64,
) -> u64 {
    if poll_interval_seconds <= 1 {
        return 0;
    }
    let mut hasher = DefaultHasher::new();
    source_id.hash(&mut hasher);
    collection_id.hash(&mut hasher);
    hasher.finish() % poll_interval_seconds
}

pub fn next_discovery_time(seed: &CollectionScheduleSeed, now: DateTime<Utc>) -> DateTime<Utc> {
    let interval_secs = seed.poll_interval_seconds.max(1) as i64;
    let offset_secs = seed
        .stagger_offset_seconds
        .min(seed.poll_interval_seconds.saturating_sub(1)) as i64;
    let now_secs = now.timestamp();
    let cycle_start = now_secs.div_euclid(interval_secs) * interval_secs;
    let mut next_secs = cycle_start + offset_secs;
    if next_secs <= now_secs {
        next_secs += interval_secs;
    }
    DateTime::<Utc>::from_timestamp(next_secs, 0).unwrap_or(now + TimeDelta::seconds(1))
}

fn retry_delay_for_attempt(attempt: i32) -> TimeDelta {
    let base = 5_i64.saturating_mul(2_i64.saturating_pow((attempt.max(1) - 1) as u32));
    let capped = base.min(300);
    let jitter = (attempt as i64 * 13) % 17;
    TimeDelta::seconds(capped + jitter)
}

async fn notify_task_event(client: &Client) -> Result<()> {
    client.batch_execute("NOTIFY task_events").await?;
    Ok(())
}

fn task_from_row(row: Row) -> Result<TaskRecord> {
    Ok(TaskRecord {
        task_id: row.get(0),
        source_id: row.get(1),
        collection_id: row.get(2),
        task_kind: row.get(3),
        artifact_id: row.get(4),
        task_state: row.get(5),
        payload_json: row.get(6),
        attempts: row.get(7),
        max_attempts: row.get(8),
    })
}
