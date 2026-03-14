use std::path::{Path, PathBuf};
use std::process::Command;
use std::process::Stdio;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tempfile::TempDir;
use tokio::time::sleep;

use crate::clickhouse::{ClickHouseConfig, ClickHousePublisher, raw_database_name};
use crate::db::{Db, open_database};
use crate::orchestrator::{
    force_schedule_due_now, recover_interrupted_artifacts, run_download_batch,
    run_due_discover_batch, run_parse_batch, seed_registry_schedules,
};
use crate::source_registry::SourceRegistry;

const DEFAULT_CLICKHOUSE_IMAGE: &str = "clickhouse/clickhouse-server:25.2";
const DEFAULT_PREVIEW_ROWS: usize = 5;
const SAMPLE_DOWNLOAD_LIMIT: usize = 1;
const SAMPLE_PARSE_LIMIT: usize = 1;

#[derive(Debug, Clone)]
pub struct PluginHarnessConfig {
    pub source_id: String,
    pub collection_id: Option<String>,
    pub run_for: Duration,
    pub workdir: Option<PathBuf>,
    pub keep_runtime: bool,
    pub clickhouse_image: String,
    pub preview_rows: usize,
}

impl PluginHarnessConfig {
    pub fn new(source_id: impl Into<String>) -> Self {
        Self {
            source_id: source_id.into(),
            collection_id: None,
            run_for: Duration::from_secs(30),
            workdir: None,
            keep_runtime: false,
            clickhouse_image: DEFAULT_CLICKHOUSE_IMAGE.to_string(),
            preview_rows: DEFAULT_PREVIEW_ROWS,
        }
    }
}

impl Default for PluginHarnessConfig {
    fn default() -> Self {
        Self::new("aemo.docs")
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct HarnessStatusCount {
    pub status: String,
    pub count: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct HarnessArtifactRecord {
    pub artifact_id: String,
    pub status: String,
    pub remote_uri: String,
    pub publication_timestamp: Option<String>,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct HarnessParseRunRecord {
    pub artifact_id: String,
    pub parser_version: String,
    pub status: String,
    pub row_count: Option<i64>,
    pub completed_at: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct HarnessTableSnapshot {
    pub database: String,
    pub table: String,
    pub total_rows: u64,
    pub preview_rows: Vec<Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PluginHarnessSnapshot {
    pub source_id: String,
    pub collection_id: String,
    pub run_for_seconds: u64,
    pub workdir: PathBuf,
    pub sqlite_path: PathBuf,
    pub clickhouse_url: String,
    pub clickhouse_container_name: String,
    pub discovery_rounds: usize,
    pub queued_to_fetch_count: usize,
    pub downloaded_artifact_count: usize,
    pub parsed_artifact_count: usize,
    pub parsed_row_count: usize,
    pub artifact_statuses: Vec<HarnessStatusCount>,
    pub artifacts: Vec<HarnessArtifactRecord>,
    pub parse_runs: Vec<HarnessParseRunRecord>,
    pub raw_tables: Vec<HarnessTableSnapshot>,
    pub semantic_tables: Vec<HarnessTableSnapshot>,
}

#[derive(Debug, Deserialize)]
struct ClickHouseSystemTableRow {
    database: String,
    name: String,
    total_rows: Option<Value>,
}

pub async fn run_plugin_harness(config: PluginHarnessConfig) -> Result<PluginHarnessSnapshot> {
    let root = HarnessRoot::new(config.workdir.clone())?;
    let registry = SourceRegistry::new();
    let collection_id = resolve_collection_id(
        &registry,
        &config.source_id,
        config.collection_id.as_deref(),
    )?;
    let registry = registry.filter_to(&config.source_id, Some(&collection_id))?;

    let mut clickhouse = ClickHouseRuntime::start(
        &config.clickhouse_image,
        &config.source_id,
        config.keep_runtime,
    )?;
    clickhouse.wait_until_ready().await?;
    clickhouse.provision_users(&config.source_id)?;

    let sqlite_path = root.path().join("data").join("historian.db");
    let data_dir = sqlite_path
        .parent()
        .ok_or_else(|| anyhow!("failed to resolve harness data dir"))?
        .to_path_buf();
    std::fs::create_dir_all(&data_dir)
        .with_context(|| format!("creating harness data dir {}", data_dir.display()))?;

    let db = open_database(&sqlite_path)?;
    let publisher = ClickHousePublisher::new(clickhouse.historian_config())?;
    publisher.ensure_ready().await?;

    let http_client = reqwest::Client::builder()
        .user_agent("energyhistorian-plugin-harness/0.1")
        .timeout(Duration::from_secs(120))
        .build()
        .context("building plugin harness HTTP client")?;

    seed_registry_schedules(&db, &registry).await?;
    recover_interrupted_artifacts(&db, &data_dir).await?;
    force_schedule_due_now(&db, &config.source_id, &collection_id).await?;

    let deadline = Instant::now() + config.run_for;
    let mut discovery_rounds = 0usize;
    let mut queued_to_fetch_count = 0usize;

    while Instant::now() < deadline {
        let discovered = run_due_discover_batch(&db, &registry, &http_client).await?;
        if discovered > 0 {
            discovery_rounds += 1;
        }
        queued_to_fetch_count = count_downloadable(&db).await?;

        if queued_to_fetch_count > 0 {
            break;
        }

        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        sleep(remaining.min(Duration::from_secs(2))).await;
    }

    let downloaded_artifact_count = run_download_batch(
        &db,
        &registry,
        &http_client,
        &data_dir,
        SAMPLE_DOWNLOAD_LIMIT,
    )
    .await?;
    let parsed_row_count = run_parse_batch(&db, &registry, &publisher, SAMPLE_PARSE_LIMIT).await?;
    let parsed_artifact_count = count_succeeded_parse_runs(&db).await?;

    let snapshot = collect_snapshot(
        &db,
        &publisher,
        &config.source_id,
        &collection_id,
        &sqlite_path,
        root.path(),
        config.run_for,
        clickhouse.name.clone(),
        clickhouse.url(),
        discovery_rounds,
        queued_to_fetch_count,
        downloaded_artifact_count,
        parsed_artifact_count,
        parsed_row_count,
        config.preview_rows,
    )
    .await?;

    if config.keep_runtime {
        clickhouse.keep();
    }
    if config.keep_runtime || config.workdir.is_some() {
        root.keep();
    }

    Ok(snapshot)
}

async fn collect_snapshot(
    db: &Db,
    publisher: &ClickHousePublisher,
    source_id: &str,
    collection_id: &str,
    sqlite_path: &Path,
    workdir: &Path,
    run_for: Duration,
    clickhouse_container_name: String,
    clickhouse_url: String,
    discovery_rounds: usize,
    queued_to_fetch_count: usize,
    downloaded_artifact_count: usize,
    parsed_artifact_count: usize,
    parsed_row_count: usize,
    preview_rows: usize,
) -> Result<PluginHarnessSnapshot> {
    let artifact_statuses = load_artifact_statuses(db).await?;
    let artifacts = load_artifacts(db).await?;
    let parse_runs = load_parse_runs(db).await?;
    let raw_tables =
        load_table_snapshots(publisher, &[raw_database_name(source_id)], preview_rows).await?;
    let semantic_tables =
        load_table_snapshots(publisher, &[String::from("semantic")], preview_rows).await?;

    Ok(PluginHarnessSnapshot {
        source_id: source_id.to_string(),
        collection_id: collection_id.to_string(),
        run_for_seconds: run_for.as_secs(),
        workdir: workdir.to_path_buf(),
        sqlite_path: sqlite_path.to_path_buf(),
        clickhouse_url,
        clickhouse_container_name,
        discovery_rounds,
        queued_to_fetch_count,
        downloaded_artifact_count,
        parsed_artifact_count,
        parsed_row_count,
        artifact_statuses,
        artifacts,
        parse_runs,
        raw_tables,
        semantic_tables,
    })
}

async fn load_artifact_statuses(db: &Db) -> Result<Vec<HarnessStatusCount>> {
    let conn = db.lock().await;
    let mut stmt =
        conn.prepare("SELECT status, count(*) FROM artifacts GROUP BY status ORDER BY status")?;
    let rows = stmt
        .query_map([], |row| {
            Ok(HarnessStatusCount {
                status: row.get(0)?,
                count: row.get(1)?,
            })
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(rows)
}

async fn load_artifacts(db: &Db) -> Result<Vec<HarnessArtifactRecord>> {
    let conn = db.lock().await;
    let mut stmt = conn.prepare(
        "SELECT artifact_id, status, remote_uri, publication_timestamp, updated_at
         FROM artifacts
         ORDER BY updated_at DESC
         LIMIT 20",
    )?;
    let rows = stmt
        .query_map([], |row| {
            Ok(HarnessArtifactRecord {
                artifact_id: row.get(0)?,
                status: row.get(1)?,
                remote_uri: row.get(2)?,
                publication_timestamp: row.get(3)?,
                updated_at: row.get(4)?,
            })
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(rows)
}

async fn load_parse_runs(db: &Db) -> Result<Vec<HarnessParseRunRecord>> {
    let conn = db.lock().await;
    let mut stmt = conn.prepare(
        "SELECT artifact_id, parser_version, status, row_count, completed_at
         FROM parse_runs
         ORDER BY completed_at DESC
         LIMIT 20",
    )?;
    let rows = stmt
        .query_map([], |row| {
            Ok(HarnessParseRunRecord {
                artifact_id: row.get(0)?,
                parser_version: row.get(1)?,
                status: row.get(2)?,
                row_count: row.get(3)?,
                completed_at: row.get(4)?,
            })
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(rows)
}

async fn count_downloadable(db: &Db) -> Result<usize> {
    let conn = db.lock().await;
    let count: i64 = conn.query_row(
        "SELECT count(*) FROM artifacts WHERE status = 'discovered'",
        [],
        |row| row.get(0),
    )?;
    Ok(count.max(0) as usize)
}

async fn count_succeeded_parse_runs(db: &Db) -> Result<usize> {
    let conn = db.lock().await;
    let count: i64 = conn.query_row(
        "SELECT count(*) FROM parse_runs WHERE status = 'succeeded'",
        [],
        |row| row.get(0),
    )?;
    Ok(count.max(0) as usize)
}

async fn load_table_snapshots(
    publisher: &ClickHousePublisher,
    databases: &[String],
    preview_rows: usize,
) -> Result<Vec<HarnessTableSnapshot>> {
    if databases.is_empty() {
        return Ok(Vec::new());
    }

    let database_list = databases
        .iter()
        .map(|database| format!("'{}'", database.replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT database, name, total_rows
         FROM system.tables
         WHERE database IN ({database_list})
         ORDER BY database, name"
    );
    let tables = publisher
        .query_json_rows::<ClickHouseSystemTableRow>(&sql)
        .await?;

    let mut snapshots = Vec::new();
    for table in tables {
        let preview_sql = format!(
            "SELECT * FROM {}.{} LIMIT {}",
            table.database, table.name, preview_rows
        );
        let preview = publisher
            .query_json_rows::<Value>(&preview_sql)
            .await
            .unwrap_or_default();
        snapshots.push(HarnessTableSnapshot {
            database: table.database,
            table: table.name,
            total_rows: parse_total_rows(table.total_rows.as_ref()).unwrap_or(preview.len() as u64),
            preview_rows: preview,
        });
    }
    Ok(snapshots)
}

fn parse_total_rows(value: Option<&Value>) -> Option<u64> {
    match value {
        Some(Value::Number(number)) => number.as_u64(),
        Some(Value::String(text)) => text.parse().ok(),
        _ => None,
    }
}

fn resolve_collection_id(
    registry: &SourceRegistry,
    source_id: &str,
    collection_id: Option<&str>,
) -> Result<String> {
    let plan = registry.source_plan(source_id)?;
    if let Some(collection_id) = collection_id {
        let found = plan
            .collections
            .iter()
            .any(|collection| collection.id == collection_id);
        if !found {
            let available = plan
                .collections
                .iter()
                .map(|collection| collection.id.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            bail!(
                "source '{source_id}' has no collection '{collection_id}'. available: {available}"
            );
        }
        return Ok(collection_id.to_string());
    }

    match plan.collections.as_slice() {
        [collection] => Ok(collection.id.clone()),
        [] => bail!("source '{source_id}' has no collections"),
        many => bail!(
            "source '{source_id}' has multiple collections; choose one of: {}",
            many.iter()
                .map(|collection| collection.id.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

struct HarnessRoot {
    path: PathBuf,
    tempdir: Option<TempDir>,
}

impl HarnessRoot {
    fn new(path: Option<PathBuf>) -> Result<Self> {
        if let Some(path) = path {
            std::fs::create_dir_all(&path)
                .with_context(|| format!("creating harness workdir {}", path.display()))?;
            return Ok(Self {
                path,
                tempdir: None,
            });
        }

        let tempdir = tempfile::Builder::new()
            .prefix("energyhistorian-plugin-harness-")
            .tempdir()
            .context("creating temporary harness workdir")?;
        let path = tempdir.path().to_path_buf();
        Ok(Self {
            path,
            tempdir: Some(tempdir),
        })
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn keep(self) {
        if let Some(tempdir) = self.tempdir {
            std::mem::forget(tempdir);
        }
    }
}

struct ClickHouseRuntime {
    name: String,
    http_port: u16,
    admin_user: String,
    admin_password: String,
    historian_user: String,
    historian_password: String,
    keep_runtime: bool,
}

impl ClickHouseRuntime {
    fn start(image: &str, source_id: &str, keep_runtime: bool) -> Result<Self> {
        let suffix = unique_suffix();
        let name = format!("energyhistorian-plugin-harness-{suffix}");
        let admin_user = format!("admin_{suffix}");
        let admin_password = format!("admin_pw_{suffix}");
        let historian_user = format!("historian_{suffix}");
        let historian_password = format!("historian_pw_{suffix}");
        let config_path = repo_path("deploy/clickhouse-config.xml");
        if !config_path.exists() {
            bail!("missing ClickHouse config at {}", config_path.display());
        }

        let mut command = Command::new("docker");
        command.arg("run").arg("-d");
        if !keep_runtime {
            command.arg("--rm");
        }
        command
            .arg("--name")
            .arg(&name)
            .arg("-e")
            .arg(format!("CLICKHOUSE_USER={admin_user}"))
            .arg("-e")
            .arg(format!("CLICKHOUSE_PASSWORD={admin_password}"))
            .arg("-e")
            .arg("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1")
            .arg("-p")
            .arg("127.0.0.1::8123")
            .arg("-v")
            .arg(format!(
                "{}:/etc/clickhouse-server/config.d/memory.xml:ro",
                config_path.display()
            ))
            .arg(image);
        run_command(
            &mut command,
            &format!("starting ClickHouse container '{name}'"),
        )?;

        let http_port = docker_port(&name, "8123/tcp")?;
        let runtime = Self {
            name,
            http_port,
            admin_user,
            admin_password,
            historian_user,
            historian_password,
            keep_runtime,
        };

        if source_id.is_empty() {
            bail!("source_id must not be empty");
        }

        Ok(runtime)
    }

    async fn wait_until_ready(&self) -> Result<()> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .context("building ClickHouse readiness client")?;
        let started = Instant::now();
        loop {
            let response = client
                .post(self.url())
                .basic_auth(&self.admin_user, Some(&self.admin_password))
                .body("SELECT 1")
                .send()
                .await;
            if let Ok(response) = response {
                if response.status() == StatusCode::OK {
                    return Ok(());
                }
            }
            if started.elapsed() > Duration::from_secs(60) {
                bail!("timed out waiting for ClickHouse container {}", self.name);
            }
            sleep(Duration::from_secs(1)).await;
        }
    }

    fn provision_users(&self, source_id: &str) -> Result<()> {
        let script = repo_path("scripts/provision_clickhouse_users.sh");
        let raw_database = raw_database_name(source_id);
        let mut command = Command::new("/bin/sh");
        command
            .arg(script)
            .env("CLICKHOUSE_ADMIN_URL", format!("{}/", self.url()))
            .env("CLICKHOUSE_ADMIN_USER", &self.admin_user)
            .env("CLICKHOUSE_ADMIN_PASSWORD", &self.admin_password)
            .env("HISTORIAN_CH_USER", &self.historian_user)
            .env("HISTORIAN_CH_PASSWORD", &self.historian_password)
            .env("AI_API_CH_USER", "harness_read")
            .env("AI_API_CH_PASSWORD", "harness_read_pw")
            .env("RAW_DATABASES", raw_database)
            .env("SEMANTIC_DB", "semantic")
            .env("TRACKING_DB", "tracking");
        run_command(&mut command, "provisioning ClickHouse users")
    }

    fn historian_config(&self) -> ClickHouseConfig {
        ClickHouseConfig {
            url: self.url(),
            user: self.historian_user.clone(),
            password: self.historian_password.clone(),
        }
    }

    fn url(&self) -> String {
        format!("http://127.0.0.1:{}", self.http_port)
    }

    fn keep(&mut self) {
        self.keep_runtime = true;
    }
}

impl Drop for ClickHouseRuntime {
    fn drop(&mut self) {
        if self.keep_runtime {
            return;
        }
        let _ = Command::new("docker")
            .arg("rm")
            .arg("-f")
            .arg(&self.name)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

fn run_command(command: &mut Command, context: &str) -> Result<()> {
    let output = command
        .output()
        .with_context(|| format!("failed to execute {context}"))?;
    if output.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    Err(anyhow!(
        "{context} failed with status {}.\nstdout:\n{}\nstderr:\n{}",
        output.status,
        stdout.trim(),
        stderr.trim()
    ))
}

fn docker_port(container_name: &str, container_port: &str) -> Result<u16> {
    let output = Command::new("docker")
        .arg("port")
        .arg(container_name)
        .arg(container_port)
        .output()
        .with_context(|| format!("querying docker port for {container_name}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "failed to resolve docker port for {container_name}: {}",
            stderr.trim()
        );
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout
        .lines()
        .find_map(|line| line.rsplit(':').next())
        .ok_or_else(|| anyhow!("docker port output missing host port for {container_name}"))?
        .trim()
        .parse::<u16>()
        .with_context(|| format!("parsing docker port output for {container_name}"))
}

fn repo_path(relative: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(relative)
}

fn unique_suffix() -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("{timestamp}_{}", std::process::id())
}
