use std::sync::Arc;

use crate::{
    auth::WorkosAuth, clickhouse::ClickHouseClient, config::Config, db::Store, llm::LlmClient,
};

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub auth: Arc<WorkosAuth>,
    pub store: Arc<Store>,
    pub clickhouse: Arc<ClickHouseClient>,
    pub llm: Arc<LlmClient>,
}
