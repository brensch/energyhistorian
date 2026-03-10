use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::artifact::{DiscoveredArtifact, LocalArtifact};
use crate::promotion::PromotionMapping;
use crate::registry::SourceDescriptor;
use crate::schema::ObservedSchema;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunContext {
    pub run_id: String,
    pub environment: String,
    pub parser_version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginCapabilities {
    pub supports_backfill: bool,
    pub supports_schema_registry: bool,
    pub supports_historical_media: bool,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceCollection {
    pub id: String,
    pub display_name: String,
    pub description: String,
    pub retrieval_modes: Vec<String>,
    pub completion: CollectionCompletion,
    pub task_blueprints: Vec<TaskBlueprint>,
    pub default_poll_interval_seconds: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceMetadataDocument {
    pub id: String,
    pub title: String,
    pub version: Option<String>,
    pub document_type: String,
    pub url: String,
    pub notes: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryRequest {
    pub collection: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromotionSpec {
    pub source_logical_table: &'static str,
    pub canonical_dataset: &'static str,
    pub mapping_name: &'static str,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CompletionUnit {
    Artifact,
    Partition,
    DocumentVersion,
    MediaImage,
    ApiCursor,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionCompletion {
    pub unit: CompletionUnit,
    pub dedupe_keys: Vec<String>,
    pub cursor_field: Option<String>,
    pub mutable_window_seconds: Option<u64>,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TaskKind {
    Discover,
    Fetch,
    Parse,
    RegisterSchema,
    ReconcileRawStorage,
    ReconcileSemanticViews,
    Promote,
    SyncMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskBlueprint {
    pub kind: TaskKind,
    pub description: String,
    pub max_concurrency: usize,
    pub queue: String,
    pub idempotency_scope: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParseResult {
    pub observed_schemas: Vec<ObservedSchema>,
    pub raw_outputs: Vec<RawTableChunk>,
    pub promotions: Vec<PromotionMapping>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawTableChunk {
    pub logical_table_key: String,
    pub schema_key: String,
    pub rows: Vec<Value>,
}

pub trait SourcePlugin {
    fn descriptor(&self) -> SourceDescriptor;
    fn capabilities(&self) -> PluginCapabilities;
    fn collections(&self) -> Vec<SourceCollection>;
    fn metadata_catalog(&self) -> Vec<SourceMetadataDocument>;
    fn discover(
        &self,
        request: &DiscoveryRequest,
        ctx: &RunContext,
    ) -> Result<Vec<DiscoveredArtifact>>;
    fn fetch(&self, artifact: &DiscoveredArtifact, ctx: &RunContext) -> Result<LocalArtifact>;
    fn parse(&self, artifact: &LocalArtifact, ctx: &RunContext) -> Result<ParseResult>;
    fn promotion_plan(&self) -> &'static [PromotionSpec];

    fn completion_for_collection(&self, collection_id: &str) -> Option<CollectionCompletion> {
        self.collections()
            .into_iter()
            .find(|collection| collection.id == collection_id)
            .map(|collection| collection.completion)
    }

    fn tasks_for_collection(&self, collection_id: &str) -> Vec<TaskBlueprint> {
        self.collections()
            .into_iter()
            .find(|collection| collection.id == collection_id)
            .map(|collection| collection.task_blueprints)
            .unwrap_or_default()
    }
}
