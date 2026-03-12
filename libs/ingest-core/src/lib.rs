pub mod artifact;
pub mod plugin;
pub mod promotion;
pub mod raw_plugin;
pub mod raw_storage;
pub mod registry;
pub mod schema;
pub mod schema_registry;
pub mod semantic;
pub mod source_family;
pub mod state;
pub mod type_inference;

pub use artifact::{ArtifactId, ArtifactKind, ArtifactMetadata, DiscoveredArtifact, LocalArtifact};
pub use plugin::{
    BoxedFuture, CollectionCompletion, CompletionUnit, DiscoveryCursorHint, DiscoveryRequest,
    ParseResult, PluginCapabilities, PromotionSpec, RawTableChunk, RawTableRow, RawTableRowSink,
    RunContext, RuntimePluginParseResult, RuntimeSourcePlugin, SourceCollection,
    SourceMetadataDocument, SourcePlugin, TaskBlueprint, TaskKind,
};
pub use promotion::{CanonicalDataset, PromotionMapping, PromotionMode};
pub use raw_plugin::{RawPluginParseResult, RawPluginTableBatch};
pub use raw_storage::{
    RawTablePlan, physical_raw_table_name, plan_raw_table, plan_raw_table_in_database,
};
pub use registry::{PluginCatalog, SourceDescriptor};
pub use schema::{
    LogicalTableId, ObservedSchema, SchemaApprovalStatus, SchemaColumn, SchemaObservationId,
    SchemaVersionKey,
};
pub use schema_registry::{FileSchemaRegistry, SchemaRegistrationOutcome};
pub use semantic::{SemanticJob, SemanticNamingStrategy};
pub use source_family::{SourceFamily, SourceFamilyCatalogEntry};
pub use state::{ArtifactProcessingStatus, ProcessingCheckpoint, ProcessingEvent};
pub use type_inference::ColumnTypeInference;
