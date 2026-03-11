use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use controlplane::{ObjectStoreConfig, connect_postgres};
use futures_util::stream::{self, FuturesUnordered, StreamExt};
use ingest_core::{LocalArtifact, RawTableRow, RawTableRowSink};
use serde_json::json;
use tempfile::tempdir;
use tokio::sync::mpsc;
use tokio_postgres::Client;
use tracing::info;

use crate::clickhouse::{ClickHouseConfig, ClickHousePublisher, MAX_INSERT_BYTES, MAX_INSERT_ROWS};
use crate::object_store::ObjectStore;
use crate::queue::{
    CollectionScheduleSeed, TaskRecord, claim_tasks, complete_task, connect_listener,
    enqueue_due_discover_tasks, fail_task, fetch_discovered_artifact, fetch_stored_artifact,
    mark_discover_failure, mark_discover_success, parse_already_succeeded, record_discoveries,
    record_fetch_success, record_parse_success, renew_task_lease, requeue_expired_leases,
    sync_collection_schedules, try_acquire_scheduler_leadership, wait_for_trigger,
};
use crate::source_registry::{ParsedArtifact, SourceRegistry};

const LEASE_DURATION: Duration = Duration::from_secs(300);

fn claimed_task_limit(claim_batch_size: usize, max_concurrency: usize) -> i64 {
    claim_batch_size.min(max_concurrency).max(1) as i64
}

pub async fn run_scheduler_service(
    postgres_client: Client,
    postgres_url: &str,
    poll_interval: Duration,
    claim_batch_size: usize,
    max_concurrency: usize,
    registry: SourceRegistry,
    schedule_seeds: Vec<CollectionScheduleSeed>,
) -> Result<()> {
    let mut worker_client = postgres_client;
    let http_client = reqwest::Client::builder()
        .user_agent("energyhistorian-scheduler/0.1")
        .timeout(Duration::from_secs(60))
        .build()?;
    let mut trigger_rx = connect_listener(postgres_url).await?;
    let worker_id = worker_id("schedulerd");

    loop {
        if !try_acquire_scheduler_leadership(&worker_client).await? {
            wait_for_trigger(&mut trigger_rx, poll_interval).await;
            continue;
        }

        sync_collection_schedules(&worker_client, &schedule_seeds).await?;
        let enqueued = enqueue_due_discover_tasks(&worker_client, &schedule_seeds).await?;
        requeue_expired_leases(&worker_client).await?;
        let tasks = claim_tasks(
            &mut worker_client,
            "discover",
            &worker_id,
            claimed_task_limit(claim_batch_size, max_concurrency),
            LEASE_DURATION,
        )
        .await?;
        info!(
            schedule_seed_count = schedule_seeds.len(),
            discover_tasks_enqueued = enqueued,
            claimed_discover_tasks = tasks.len(),
            "scheduler loop tick"
        );
        if tasks.is_empty() {
            wait_for_trigger(&mut trigger_rx, poll_interval).await;
            continue;
        }

        let results = stream::iter(tasks.into_iter().map(|task| {
            let postgres_url = postgres_url.to_string();
            let http_client = http_client.clone();
            let registry = registry.clone();
            let worker_id = worker_id.clone();
            async move {
                let client = connect_postgres(&postgres_url).await?;
                let heartbeat_client = connect_postgres(&postgres_url).await?;
                let heartbeat = spawn_lease_heartbeat(
                    heartbeat_client,
                    task.task_id.clone(),
                    worker_id,
                    LEASE_DURATION,
                );
                let result = execute_discover_task(&client, &http_client, &registry, &task).await;
                heartbeat.stop().await;
                Ok::<_, anyhow::Error>((task, result))
            }
        }))
        .buffer_unordered(max_concurrency.max(1))
        .collect::<Vec<_>>()
        .await;

        for result in results {
            let (task, result) = result?;
            match result {
                Ok(()) => {
                    mark_discover_success(&worker_client, &task.source_id, &task.collection_id)
                        .await?;
                    complete_task(&worker_client, &task.task_id).await?;
                }
                Err(error) => {
                    mark_discover_failure(&worker_client, &task.source_id, &task.collection_id)
                        .await?;
                    fail_task(&worker_client, &task, &format!("{error:#}")).await?;
                }
            }
        }
    }
}

pub async fn run_downloader_service(
    mut postgres_client: Client,
    postgres_url: &str,
    object_store_config: ObjectStoreConfig,
    poll_interval: Duration,
    claim_batch_size: usize,
    max_concurrency: usize,
    registry: SourceRegistry,
) -> Result<()> {
    let http_client = reqwest::Client::builder()
        .user_agent("energyhistorian-downloader/0.1")
        .timeout(Duration::from_secs(60))
        .build()?;
    let object_store = ObjectStore::new(object_store_config).await?;
    let mut trigger_rx = connect_listener(postgres_url).await?;
    let worker_id = worker_id("downloaderd");
    let concurrency = max_concurrency.max(1);
    let mut active = FuturesUnordered::new();

    loop {
        while active.len() < concurrency {
            requeue_expired_leases(&postgres_client).await?;
            let available_slots = concurrency - active.len();
            let tasks = claim_tasks(
                &mut postgres_client,
                "fetch",
                &worker_id,
                claimed_task_limit(claim_batch_size, available_slots),
                LEASE_DURATION,
            )
            .await?;
            if !tasks.is_empty() {
                info!(
                    claimed_fetch_tasks = tasks.len(),
                    active_fetch_tasks = active.len(),
                    "downloader claimed tasks"
                );
            }
            if tasks.is_empty() {
                break;
            }
            for task in tasks {
                let postgres_url = postgres_url.to_string();
                let http_client = http_client.clone();
                let object_store = object_store.clone();
                let registry = registry.clone();
                let worker_id = worker_id.clone();
                active.push(async move {
                    let client = connect_postgres(&postgres_url).await?;
                    let heartbeat_client = connect_postgres(&postgres_url).await?;
                    let heartbeat = spawn_lease_heartbeat(
                        heartbeat_client,
                        task.task_id.clone(),
                        worker_id,
                        LEASE_DURATION,
                    );
                    let result =
                        execute_fetch_task(&client, &http_client, &object_store, &registry, &task)
                            .await;
                    heartbeat.stop().await;
                    Ok::<_, anyhow::Error>((task, result))
                });
            }
        }
        if active.is_empty() {
            wait_for_trigger(&mut trigger_rx, poll_interval).await;
            continue;
        }

        if let Some(result) = active.next().await {
            let (task, result) = result?;
            match result {
                Ok(()) => complete_task(&postgres_client, &task.task_id).await?,
                Err(error) => fail_task(&postgres_client, &task, &format!("{error:#}")).await?,
            }
        }
    }
}

pub async fn run_parser_service(
    mut postgres_client: Client,
    postgres_url: &str,
    object_store_config: ObjectStoreConfig,
    clickhouse: ClickHouseConfig,
    poll_interval: Duration,
    claim_batch_size: usize,
    max_concurrency: usize,
    registry: SourceRegistry,
) -> Result<()> {
    let object_store = ObjectStore::new(object_store_config).await?;
    let publisher = ClickHousePublisher::new(clickhouse)?;
    publisher.ensure_ready().await?;
    let mut trigger_rx = connect_listener(postgres_url).await?;
    let worker_id = worker_id("parserd");
    let concurrency = max_concurrency.max(1);
    let mut active = FuturesUnordered::new();

    loop {
        while active.len() < concurrency {
            requeue_expired_leases(&postgres_client).await?;
            let available_slots = concurrency - active.len();
            let tasks = claim_tasks(
                &mut postgres_client,
                "parse",
                &worker_id,
                claimed_task_limit(claim_batch_size, available_slots),
                LEASE_DURATION,
            )
            .await?;
            if !tasks.is_empty() {
                info!(
                    claimed_parse_tasks = tasks.len(),
                    active_parse_tasks = active.len(),
                    "parser claimed tasks"
                );
            }
            if tasks.is_empty() {
                break;
            }
            for task in tasks {
                let postgres_url = postgres_url.to_string();
                let object_store = object_store.clone();
                let publisher = publisher.clone();
                let registry = registry.clone();
                let worker_id = worker_id.clone();
                active.push(async move {
                    let client = connect_postgres(&postgres_url).await?;
                    let heartbeat_client = connect_postgres(&postgres_url).await?;
                    let heartbeat = spawn_lease_heartbeat(
                        heartbeat_client,
                        task.task_id.clone(),
                        worker_id,
                        LEASE_DURATION,
                    );
                    let result =
                        execute_parse_task(&client, &object_store, &publisher, &registry, &task)
                            .await;
                    heartbeat.stop().await;
                    Ok::<_, anyhow::Error>((task, result))
                });
            }
        }
        if active.is_empty() {
            wait_for_trigger(&mut trigger_rx, poll_interval).await;
            continue;
        }

        if let Some(result) = active.next().await {
            let (task, result) = result?;
            match result {
                Ok(()) => complete_task(&postgres_client, &task.task_id).await?,
                Err(error) => fail_task(&postgres_client, &task, &format!("{error:#}")).await?,
            }
        }
    }
}

async fn execute_discover_task(
    client: &Client,
    http_client: &reqwest::Client,
    registry: &SourceRegistry,
    task: &TaskRecord,
) -> Result<()> {
    let limit = task
        .payload_json
        .get("limit")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(10) as usize;
    let discoveries = registry
        .discover(http_client, &task.source_id, &task.collection_id, limit)
        .await?;
    record_discoveries(client, &task.source_id, &task.collection_id, &discoveries).await?;
    Ok(())
}

async fn execute_fetch_task(
    client: &Client,
    http_client: &reqwest::Client,
    object_store: &ObjectStore,
    registry: &SourceRegistry,
    task: &TaskRecord,
) -> Result<()> {
    let artifact_id = task
        .artifact_id
        .as_deref()
        .ok_or_else(|| anyhow!("fetch task missing artifact_id"))?;
    info!(
        task_id = %task.task_id,
        artifact_id = artifact_id,
        source_id = %task.source_id,
        collection_id = %task.collection_id,
        "starting fetch task"
    );
    if let Ok(existing) = fetch_stored_artifact(client, artifact_id).await {
        let discovered = fetch_discovered_artifact(client, artifact_id).await?;
        record_fetch_success(
            client,
            task,
            &existing.bucket,
            &existing.key,
            existing
                .content_type
                .as_deref()
                .unwrap_or("application/octet-stream"),
            &existing.content_sha256,
            existing.content_length_bytes,
            None,
            &discovered.metadata.parser_version,
        )
        .await?;
        info!(
            task_id = %task.task_id,
            artifact_id = artifact_id,
            object_key = %existing.key,
            content_length_bytes = existing.content_length_bytes,
            "fetch task reused stored artifact"
        );
        return Ok(());
    }
    let discovered = fetch_discovered_artifact(client, artifact_id).await?;
    let temp_dir = tempdir()?;
    let local = registry
        .fetch(
            http_client,
            &task.source_id,
            &task.collection_id,
            &discovered,
            temp_dir.path(),
        )
        .await?;
    let key = object_store.artifact_key(
        &task.source_id,
        &task.collection_id,
        artifact_id,
        &local.metadata.acquisition_uri,
    );
    let stored = object_store.put_path(&key, &local.local_path).await?;
    record_fetch_success(
        client,
        task,
        &stored.bucket,
        &stored.key,
        &stored.content_type,
        &stored.sha256,
        stored.content_length_bytes as i64,
        stored.etag.as_deref(),
        &local.metadata.parser_version,
    )
    .await?;
    info!(
        task_id = %task.task_id,
        artifact_id = artifact_id,
        object_key = %stored.key,
        content_length_bytes = stored.content_length_bytes,
        "fetch task stored artifact"
    );
    Ok(())
}

async fn execute_parse_task(
    client: &Client,
    object_store: &ObjectStore,
    publisher: &ClickHousePublisher,
    registry: &SourceRegistry,
    task: &TaskRecord,
) -> Result<()> {
    let artifact_id = task
        .artifact_id
        .as_deref()
        .ok_or_else(|| anyhow!("parse task missing artifact_id"))?;
    let parser_version = task
        .payload_json
        .get("parser_version")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("parserd/0.1");
    info!(
        task_id = %task.task_id,
        artifact_id = artifact_id,
        parser_version = parser_version,
        source_id = %task.source_id,
        collection_id = %task.collection_id,
        "starting parse task"
    );
    if parse_already_succeeded(client, artifact_id, parser_version).await? {
        info!(
            task_id = %task.task_id,
            artifact_id = artifact_id,
            parser_version = parser_version,
            "parse task already succeeded"
        );
        return Ok(());
    }
    let discovered = fetch_discovered_artifact(client, artifact_id).await?;
    let stored = fetch_stored_artifact(client, artifact_id).await?;
    let temp_dir = tempdir()?;
    let local_path = temp_dir.path().join(
        discovered
            .metadata
            .acquisition_uri
            .rsplit('/')
            .next()
            .unwrap_or("artifact.bin"),
    );
    let content_type = stored
        .content_type
        .as_deref()
        .unwrap_or("application/octet-stream");
    let artifact_path = object_store
        .fetch_to_path(&stored.key, &local_path, content_type)
        .await?;
    let artifact = LocalArtifact {
        metadata: discovered.metadata,
        local_path: artifact_path,
    };
    let parsed = registry.parse(&task.source_id, &task.collection_id, artifact)?;
    match parsed {
        ParsedArtifact::StructuredRaw { artifact, result } => {
            let published = publish_structured_raw_rows(
                publisher,
                registry,
                &task.source_id,
                &task.collection_id,
                &artifact,
                &result,
            )
            .await?;
            record_parse_success(
                client,
                task,
                "parserd",
                parser_version,
                published as i64,
                json!({ "clickhouse_rows": published }),
            )
            .await?;
            info!(
                task_id = %task.task_id,
                artifact_id = artifact_id,
                collection_id = %task.collection_id,
                clickhouse_rows = published,
                observed_schema_count = result.observed_schemas.len(),
                "completed parse task"
            );
        }
        ParsedArtifact::RawMetadata { artifact, result } => {
            let published = publisher
                .publish_raw_plugin_parse_result(
                    &task.source_id,
                    &task.collection_id,
                    &artifact,
                    &result,
                )
                .await?;
            record_parse_success(
                client,
                task,
                "parserd",
                parser_version,
                published as i64,
                json!({ "clickhouse_rows": published }),
            )
            .await?;
            info!(
                task_id = %task.task_id,
                artifact_id = artifact_id,
                collection_id = %task.collection_id,
                clickhouse_rows = published,
                raw_table_count = result.tables.len(),
                "completed parse task"
            );
        }
    }
    Ok(())
}

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
    let collection_id = collection_id.to_string();
    let parse_collection_id = collection_id.clone();
    let registry2 = registry.clone();
    let parse_source_id = source_id.to_string();
    let parse_handle = tokio::task::spawn_blocking(move || -> Result<()> {
        let mut sink = ChannelRawTableRowSink { tx };
        registry2
            .stream_structured_parse(
                &parse_source_id,
                &artifact2,
                &parse_collection_id,
                &mut sink,
            )
            .with_context(|| {
                format!(
                    "streaming structured raw parsed rows for {}",
                    parse_source_id
                )
            })
    });

    let mut batcher = ClickHouseRowBatcher::new(
        publisher,
        &result.observed_schemas,
        source_id,
        collection_id,
        &artifact.metadata.artifact_id,
        &artifact.metadata.acquisition_uri,
        chrono::Utc::now()
            .format("%Y-%m-%d %H:%M:%S%.3f")
            .to_string(),
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
    rows: Vec<serde_json::Value>,
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
        processed_at: String,
    ) -> Self {
        let _ = processed_at;
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

fn worker_id(service: &str) -> String {
    let hostname = std::env::var("HOSTNAME").unwrap_or_else(|_| "local".to_string());
    format!("{service}:{hostname}:{}", std::process::id())
}

struct LeaseHeartbeat {
    stop_tx: Option<tokio::sync::oneshot::Sender<()>>,
    handle: tokio::task::JoinHandle<()>,
}

impl LeaseHeartbeat {
    async fn stop(mut self) {
        if let Some(stop_tx) = self.stop_tx.take() {
            let _ = stop_tx.send(());
        }
        let _ = self.handle.await;
    }
}

fn spawn_lease_heartbeat(
    client: Client,
    task_id: String,
    worker_id: String,
    lease_duration: Duration,
) -> LeaseHeartbeat {
    let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel();
    let interval = lease_duration
        .checked_div(2)
        .unwrap_or_else(|| Duration::from_secs(30))
        .max(Duration::from_secs(30));
    let handle = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.tick().await;
        loop {
            tokio::select! {
                _ = &mut stop_rx => break,
                _ = ticker.tick() => {
                    match renew_task_lease(&client, &task_id, &worker_id, lease_duration).await {
                        Ok(true) => {}
                        Ok(false) => break,
                        Err(error) => {
                            tracing::warn!(task_id = %task_id, error = ?error, "lease heartbeat failed");
                            break;
                        }
                    }
                }
            }
        }
    });
    LeaseHeartbeat {
        stop_tx: Some(stop_tx),
        handle,
    }
}
