use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use controlplane::{
    HealthState, ObjectStoreConfig, ServiceConfig, ServiceRole, connect_postgres,
    spawn_health_server,
};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(author, version, about = "energyhistorian parser service")]
struct Args {
    #[arg(long, env = "LISTEN_ADDR", default_value = "0.0.0.0:8080")]
    listen_addr: SocketAddr,
    #[arg(long, env = "POSTGRES_URL")]
    postgres_url: String,
    #[arg(long, env = "S3_ENDPOINT")]
    s3_endpoint: String,
    #[arg(long, env = "S3_REGION", default_value = "us-east-1")]
    s3_region: String,
    #[arg(long, env = "S3_BUCKET")]
    s3_bucket: String,
    #[arg(long, env = "S3_ACCESS_KEY_ID")]
    s3_access_key_id: String,
    #[arg(long, env = "S3_SECRET_ACCESS_KEY")]
    s3_secret_access_key: String,
    #[arg(long, env = "S3_FORCE_PATH_STYLE", default_value_t = true)]
    s3_force_path_style: bool,
    #[arg(long, env = "POLL_INTERVAL_SECS", default_value_t = 5)]
    poll_interval_secs: u64,
    #[arg(long, env = "CLAIM_BATCH_SIZE", default_value_t = 10)]
    claim_batch_size: usize,
    #[arg(long, env = "MAX_CONCURRENCY", default_value_t = 4)]
    max_concurrency: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_logging();
    let args = Args::parse();
    let config = build_config(args, ServiceRole::Parser);
    let health = HealthState::default();
    let health_server = spawn_health_server(config.role, config.listen_addr, health.clone());
    let _db = connect_postgres(&config.postgres_url).await?;
    health.mark_startup_complete();
    health.mark_ready();

    info!(
        role = config.role.as_str(),
        listen_addr = %config.listen_addr,
        postgres_url = %config.postgres_url,
        bucket = %config.object_store.bucket,
        object_store_region = %config.object_store.region,
        "service configured"
    );
    info!(
        poll_interval_secs = config.poll_interval.as_secs(),
        claim_batch_size = config.claim_batch_size,
        max_concurrency = config.max_concurrency,
        "parser service scaffold ready"
    );

    health_server.await.context("health server task panicked")?
}

fn build_config(args: Args, role: ServiceRole) -> ServiceConfig {
    ServiceConfig {
        role,
        listen_addr: args.listen_addr,
        postgres_url: args.postgres_url,
        poll_interval: Duration::from_secs(args.poll_interval_secs),
        claim_batch_size: args.claim_batch_size,
        max_concurrency: args.max_concurrency,
        object_store: ObjectStoreConfig {
            endpoint: args.s3_endpoint,
            region: args.s3_region,
            bucket: args.s3_bucket,
            access_key_id: args.s3_access_key_id,
            secret_access_key: args.s3_secret_access_key,
            force_path_style: args.s3_force_path_style,
        },
    }
}

fn init_logging() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,parserd=info")),
        )
        .init();
}
