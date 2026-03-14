use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Instant;

use anyhow::{Context, Result};
use clap::Parser;
use tracing::info;
use tracing_subscriber::EnvFilter;

use energyhistorian::clickhouse::{ClickHouseConfig, ClickHousePublisher};
use energyhistorian::db::open_database;
use energyhistorian::health::{HealthContext, spawn_health_server};
use energyhistorian::orchestrator::{OrchestratorConfig, Stats, run_orchestrator};
use energyhistorian::source_registry::SourceRegistry;

#[derive(Debug, Parser)]
#[command(
    author,
    version,
    about = "energyhistorian – single-binary data platform"
)]
struct Args {
    /// Directory for SQLite database and downloaded artifacts
    #[arg(long, env = "DATA_DIR", default_value = "./data")]
    data_dir: PathBuf,

    /// ClickHouse HTTP URL
    #[arg(long, env = "CLICKHOUSE_URL", default_value = "http://127.0.0.1:8123")]
    clickhouse_url: String,

    /// ClickHouse user
    #[arg(long, env = "CLICKHOUSE_USER", default_value = "energyhistorian")]
    clickhouse_user: String,

    /// ClickHouse password
    #[arg(long, env = "CLICKHOUSE_PASSWORD", default_value = "energyhistorian")]
    clickhouse_password: String,

    /// Health/status HTTP listen address
    #[arg(long, env = "LISTEN_ADDR", default_value = "0.0.0.0:8080")]
    listen_addr: SocketAddr,

    /// Max concurrent discovery tasks
    #[arg(long, env = "DISCOVER_CONCURRENCY", default_value_t = 4)]
    discover_concurrency: usize,

    /// Max concurrent download tasks
    #[arg(long, env = "DOWNLOAD_CONCURRENCY", default_value_t = 4)]
    download_concurrency: usize,

    /// Max concurrent parse tasks
    #[arg(long, env = "PARSE_CONCURRENCY", default_value_t = 2)]
    parse_concurrency: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_logging();
    let args = Args::parse();

    // Ensure data directory exists
    std::fs::create_dir_all(&args.data_dir)
        .with_context(|| format!("creating data directory {}", args.data_dir.display()))?;

    let db_path = args.data_dir.join("historian.db");
    let db = open_database(&db_path)?;
    info!(db_path = %db_path.display(), "opened SQLite database");

    let publisher = ClickHousePublisher::new(ClickHouseConfig {
        url: args.clickhouse_url.clone(),
        user: args.clickhouse_user.clone(),
        password: args.clickhouse_password.clone(),
    })?;
    publisher
        .ensure_ready()
        .await
        .context("connecting to ClickHouse")?;
    info!(clickhouse_url = %args.clickhouse_url, "connected to ClickHouse");

    let registry = SourceRegistry::new();
    let seeds = registry.schedule_seeds();
    info!(
        schedule_count = seeds.len(),
        discover_concurrency = args.discover_concurrency,
        download_concurrency = args.download_concurrency,
        parse_concurrency = args.parse_concurrency,
        "starting orchestrator"
    );

    let stats = Stats::default();

    let health_ctx = HealthContext {
        db: db.clone(),
        publisher: publisher.clone(),
        stats: stats.clone(),
        started_at: Instant::now(),
    };
    let health_handle = spawn_health_server(args.listen_addr, health_ctx);
    info!(listen_addr = %args.listen_addr, "health server started");

    tokio::select! {
        result = run_orchestrator(
            db,
            registry,
            publisher,
            OrchestratorConfig {
                data_dir: args.data_dir,
                discover_concurrency: args.discover_concurrency,
                download_concurrency: args.download_concurrency,
                parse_concurrency: args.parse_concurrency,
            },
            stats,
        ) => {
            result.context("orchestrator exited")?;
        }
        result = health_handle => {
            result.context("health server panicked")??;
        }
        result = tokio::signal::ctrl_c() => {
            result.context("waiting for ctrl-c")?;
            info!("shutting down");
        }
    }

    Ok(())
}

fn init_logging() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,energyhistorian=info")),
        )
        .init();
}
