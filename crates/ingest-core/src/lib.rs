pub mod artifact;
pub mod plugin;
pub mod promotion;
pub mod raw_storage;
pub mod registry;
pub mod schema;
pub mod schema_registry;
pub mod state;
pub mod type_inference;

pub use artifact::{ArtifactId, ArtifactKind, ArtifactMetadata, DiscoveredArtifact, LocalArtifact};
pub use plugin::{
    CollectionCompletion, CompletionUnit, DiscoveryRequest, ParseResult, PluginCapabilities,
    PromotionSpec, RawTableChunk, RunContext, SourceCollection, SourceMetadataDocument,
    SourcePlugin, TaskBlueprint, TaskKind,
};
pub use promotion::{CanonicalDataset, PromotionMapping, PromotionMode};
pub use raw_storage::{RawTablePlan, physical_raw_table_name, plan_raw_table};
pub use registry::{PluginCatalog, SourceDescriptor};
pub use schema::{
    LogicalTableId, ObservedSchema, SchemaApprovalStatus, SchemaColumn, SchemaObservationId,
    SchemaVersionKey,
};
pub use schema_registry::{FileSchemaRegistry, SchemaRegistrationOutcome};
pub use state::{ArtifactProcessingStatus, ProcessingCheckpoint, ProcessingEvent};
pub use type_inference::ColumnTypeInference;
