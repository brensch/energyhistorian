use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;

use energyhistorian::plugin_harness::{PluginHarnessConfig, run_plugin_harness};

#[derive(Debug, Parser)]
#[command(
    author,
    version,
    about = "Run one source plugin end to end against fresh SQLite and ClickHouse instances"
)]
struct Args {
    /// Source/plugin id from the registry
    #[arg(long)]
    source_id: String,

    /// Collection id within the source
    #[arg(long)]
    collection_id: Option<String>,

    /// Total time to let the harness run before snapshotting
    #[arg(long, default_value_t = 30)]
    run_seconds: u64,

    /// Preserve ClickHouse container and workdir after the harness exits
    #[arg(long, default_value_t = false)]
    keep_runtime: bool,

    /// Optional working directory for SQLite data and downloaded artifacts
    #[arg(long)]
    workdir: Option<PathBuf>,

    /// ClickHouse image used for the ephemeral container
    #[arg(long, default_value = "clickhouse/clickhouse-server:25.2")]
    clickhouse_image: String,

    /// Number of rows to preview from each resulting ClickHouse table
    #[arg(long, default_value_t = 5)]
    preview_rows: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let snapshot = run_plugin_harness(PluginHarnessConfig {
        source_id: args.source_id,
        collection_id: args.collection_id,
        run_for: Duration::from_secs(args.run_seconds),
        workdir: args.workdir,
        keep_runtime: args.keep_runtime,
        clickhouse_image: args.clickhouse_image,
        preview_rows: args.preview_rows,
    })
    .await?;

    println!("{}", serde_json::to_string_pretty(&snapshot)?);
    Ok(())
}
