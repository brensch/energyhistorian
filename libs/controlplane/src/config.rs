use std::net::SocketAddr;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceRole {
    Scheduler,
    Downloader,
    Parser,
}

impl ServiceRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Scheduler => "scheduler",
            Self::Downloader => "downloader",
            Self::Parser => "parser",
        }
    }
}

#[derive(Debug, Clone)]
pub struct ObjectStoreConfig {
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub force_path_style: bool,
}

#[derive(Debug, Clone)]
pub struct ServiceConfig {
    pub role: ServiceRole,
    pub listen_addr: SocketAddr,
    pub postgres_url: String,
    pub poll_interval: Duration,
    pub claim_batch_size: usize,
    pub max_concurrency: usize,
    pub object_store: ObjectStoreConfig,
}
