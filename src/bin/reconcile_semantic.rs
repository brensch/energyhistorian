#[path = "../clickhouse.rs"]
mod clickhouse;
#[path = "../semantic.rs"]
mod semantic;
#[path = "../source_registry.rs"]
mod source_registry;

use anyhow::Result;
use clap::Parser;

use crate::clickhouse::{ClickHouseConfig, ClickHousePublisher};
use crate::semantic::reconcile_source_semantics;
use crate::source_registry::SourceRegistry;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, env = "CLICKHOUSE_URL", default_value = "http://127.0.0.1:8123")]
    clickhouse_url: String,
    #[arg(long, env = "CLICKHOUSE_USER", default_value = "energyhistorian")]
    clickhouse_user: String,
    #[arg(long, env = "CLICKHOUSE_PASSWORD", default_value = "energyhistorian")]
    clickhouse_password: String,
    #[arg(long)]
    source_id: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let publisher = ClickHousePublisher::new(ClickHouseConfig {
        url: args.clickhouse_url,
        user: args.clickhouse_user,
        password: args.clickhouse_password,
    })?;
    let registry = SourceRegistry::new();

    if let Some(source_id) = args.source_id.as_deref() {
        let jobs = reconcile_source_semantics(&publisher, &registry, source_id).await?;
        println!("{source_id}: {jobs} jobs");
        return Ok(());
    }

    let mut source_ids = registry
        .schedule_seeds()
        .into_iter()
        .map(|seed| seed.source_id)
        .collect::<Vec<_>>();
    source_ids.sort();
    source_ids.dedup();

    for source_id in source_ids {
        let jobs = reconcile_source_semantics(&publisher, &registry, &source_id).await?;
        println!("{source_id}: {jobs} jobs");
    }

    Ok(())
}
