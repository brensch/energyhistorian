use std::collections::HashMap;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::artifact::{DiscoveredArtifact, LocalArtifact};
use crate::promotion::PromotionMapping;
use crate::raw_plugin::RawPluginParseResult;
use crate::raw_value::StructuredRow;
use crate::registry::SourceDescriptor;
use crate::schema::ObservedSchema;
use crate::semantic::SemanticJob;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunContext {
    pub run_id: String,
    pub environment: String,
    pub parser_version: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DiscoveryCursorHint {
    pub latest_publication_timestamp: Option<chrono::DateTime<chrono::Utc>>,
    pub latest_release_name: Option<String>,
    pub earliest_publication_timestamp: Option<chrono::DateTime<chrono::Utc>>,
    pub earliest_release_name: Option<String>,
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
    #[serde(default)]
    pub row_count: usize,
    #[serde(default)]
    pub rows: Vec<StructuredRow>,
}

impl RawTableChunk {
    pub fn row_count(&self) -> usize {
        if self.row_count == 0 {
            self.rows.len()
        } else {
            self.row_count
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawTableRow {
    pub logical_table_key: String,
    pub schema_key: String,
    pub row: StructuredRow,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StructuredRawEvent {
    Schema(ObservedSchema),
    Row(RawTableRow),
}

pub trait RawTableRowSink {
    fn accept(&mut self, row: RawTableRow) -> Result<()>;
}

pub trait StructuredRawEventSink {
    fn accept(&mut self, event: StructuredRawEvent) -> Result<()>;
}

struct EventToRowSinkAdapter<'a> {
    sink: &'a mut dyn StructuredRawEventSink,
}

impl RawTableRowSink for EventToRowSinkAdapter<'_> {
    fn accept(&mut self, row: RawTableRow) -> Result<()> {
        self.sink.accept(StructuredRawEvent::Row(row))
    }
}

pub type BoxedFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Default)]
struct CollectingRawTableRowSink {
    order: Vec<(String, String)>,
    rows_by_output: HashMap<(String, String), Vec<StructuredRow>>,
}

impl CollectingRawTableRowSink {
    fn into_raw_outputs(self) -> Vec<RawTableChunk> {
        let mut rows_by_output = self.rows_by_output;
        self.order
            .into_iter()
            .filter_map(|(logical_table_key, schema_key)| {
                let key = (logical_table_key.clone(), schema_key.clone());
                rows_by_output.remove(&key).map(|rows| RawTableChunk {
                    logical_table_key,
                    schema_key,
                    row_count: rows.len(),
                    rows,
                })
            })
            .collect()
    }
}

impl RawTableRowSink for CollectingRawTableRowSink {
    fn accept(&mut self, row: RawTableRow) -> Result<()> {
        let key = (row.logical_table_key, row.schema_key);
        let entry = self.rows_by_output.entry(key.clone()).or_insert_with(|| {
            self.order.push(key);
            Vec::new()
        });
        entry.push(row.row);
        Ok(())
    }
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
    fn inspect_parse(&self, artifact: &LocalArtifact, ctx: &RunContext) -> Result<ParseResult>;
    fn stream_parse(
        &self,
        artifact: &LocalArtifact,
        ctx: &RunContext,
        sink: &mut dyn RawTableRowSink,
    ) -> Result<()>;
    fn parse(&self, artifact: &LocalArtifact, ctx: &RunContext) -> Result<ParseResult> {
        let mut collector = CollectingRawTableRowSink::default();
        let inspected = self.inspect_parse(artifact, ctx)?;
        self.stream_parse(artifact, ctx, &mut collector)?;
        Ok(ParseResult {
            observed_schemas: inspected.observed_schemas,
            raw_outputs: collector.into_raw_outputs(),
            promotions: inspected.promotions,
        })
    }
    fn promotion_plan(&self) -> &'static [PromotionSpec];
    fn semantic_jobs(&self) -> Vec<SemanticJob> {
        Vec::new()
    }

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RuntimePluginParseResult {
    StructuredRaw {
        artifact: LocalArtifact,
    },
    RawMetadata {
        artifact: LocalArtifact,
        result: RawPluginParseResult,
    },
}

pub trait RuntimeSourcePlugin: SourcePlugin + Send + Sync {
    fn parser_version(&self) -> &'static str;

    fn discover_collection_async<'a>(
        &'a self,
        client: &'a reqwest::Client,
        collection_id: &'a str,
        limit: usize,
        cursor: &'a DiscoveryCursorHint,
        ctx: &'a RunContext,
    ) -> BoxedFuture<'a, Result<Vec<DiscoveredArtifact>>>;

    fn fetch_artifact_async<'a>(
        &'a self,
        client: &'a reqwest::Client,
        collection_id: &'a str,
        artifact: &'a DiscoveredArtifact,
        output_dir: &'a Path,
    ) -> BoxedFuture<'a, Result<LocalArtifact>>;

    fn parse_artifact_runtime(
        &self,
        collection_id: &str,
        artifact: LocalArtifact,
        ctx: &RunContext,
    ) -> Result<RuntimePluginParseResult>;

    fn stream_structured_parse_runtime(
        &self,
        artifact: &LocalArtifact,
        collection_id: &str,
        ctx: &RunContext,
        sink: &mut dyn RawTableRowSink,
    ) -> Result<()>;

    fn stream_structured_parse_events_runtime(
        &self,
        artifact: &LocalArtifact,
        collection_id: &str,
        ctx: &RunContext,
        sink: &mut dyn StructuredRawEventSink,
    ) -> Result<()> {
        let inspected = self.inspect_parse(artifact, ctx)?;
        for schema in inspected.observed_schemas {
            sink.accept(StructuredRawEvent::Schema(schema))?;
        }
        let mut row_sink = EventToRowSinkAdapter { sink };
        self.stream_structured_parse_runtime(artifact, collection_id, ctx, &mut row_sink)
    }
}
