use anyhow::Result;
use clap::Parser;
use runtime::{ClickHouseConfig, ClickHousePublisher, SourceRegistry, reconcile_source_semantics};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(
    author,
    version,
    about = "reconcile semantic views against current ClickHouse state"
)]
struct Args {
    #[arg(long, env = "CLICKHOUSE_URL", default_value = "http://127.0.0.1:8123")]
    clickhouse_url: String,
    #[arg(long, env = "CLICKHOUSE_USER", default_value = "energyhistorian")]
    clickhouse_user: String,
    #[arg(long, env = "CLICKHOUSE_PASSWORD", default_value = "energyhistorian")]
    clickhouse_password: String,
    #[arg(long = "source", required = true)]
    sources: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_logging();
    let args = Args::parse();
    let publisher = ClickHousePublisher::new(ClickHouseConfig {
        url: args.clickhouse_url,
        user: args.clickhouse_user,
        password: args.clickhouse_password,
    })?;
    let registry = SourceRegistry::new();

    for source_id in &args.sources {
        let executed = reconcile_source_semantics(&publisher, &registry, source_id).await?;
        info!(source_id, executed, "reconciled semantic jobs");
    }

    Ok(())
}

fn init_logging() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,reconcile_semantics=info")),
        )
        .init();
}
