use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use chrono::Utc;
use ingest_core::{
    DiscoveredArtifact, LocalArtifact, RunContext, RuntimePluginParseResult, RuntimeSourcePlugin,
    SourceCollection, SourceDescriptor, SourceMetadataDocument,
};
use serde::Serialize;
use source_aemo_dvd::AemoMetadataDvdPlugin;
use source_aemo_metadata::AemoMetadataHtmlPlugin;
use source_mmsdm::MmsdmPlugin;
use source_nemweb::NemwebPlugin;

use crate::queue::{CollectionScheduleSeed, stable_stagger_offset_seconds};

#[derive(Debug, Clone, Serialize)]
pub struct SourcePlan {
    pub descriptor: SourceDescriptor,
    pub collections: Vec<SourceCollection>,
    pub metadata_documents: Vec<SourceMetadataDocument>,
}

pub type ParsedArtifact = RuntimePluginParseResult;

#[derive(Clone)]
struct RegisteredSource {
    parser_version: String,
    plan: SourcePlan,
    implementation: Arc<dyn RuntimeSourcePlugin>,
}

#[derive(Clone)]
pub struct SourceRegistry {
    sources: Vec<RegisteredSource>,
    source_indexes: HashMap<String, usize>,
}

impl Default for SourceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl SourceRegistry {
    pub fn new() -> Self {
        let mut registry = Self {
            sources: Vec::new(),
            source_indexes: HashMap::new(),
        };
        registry.register(NemwebPlugin::new());
        registry.register(MmsdmPlugin::new());
        registry.register(AemoMetadataHtmlPlugin::new());
        registry.register(AemoMetadataDvdPlugin::new());
        registry
    }

    pub fn source_plans(&self) -> Vec<SourcePlan> {
        self.sources
            .iter()
            .map(|source| source.plan.clone())
            .collect()
    }

    pub async fn discover(
        &self,
        client: &reqwest::Client,
        source_id: &str,
        collection_id: &str,
        limit: usize,
    ) -> Result<Vec<DiscoveredArtifact>> {
        let source = self.source(source_id)?;
        let ctx = self.run_context(source, "discover", collection_id);
        source
            .implementation
            .discover_collection_async(client, collection_id, limit, &ctx)
            .await
    }

    pub async fn fetch(
        &self,
        client: &reqwest::Client,
        source_id: &str,
        collection_id: &str,
        artifact: &DiscoveredArtifact,
        output_dir: &Path,
    ) -> Result<LocalArtifact> {
        let source = self.source(source_id)?;
        source
            .implementation
            .fetch_artifact_async(client, collection_id, artifact, output_dir)
            .await
    }

    pub fn parse(
        &self,
        source_id: &str,
        collection_id: &str,
        artifact: LocalArtifact,
    ) -> Result<ParsedArtifact> {
        let source = self.source(source_id)?;
        let ctx = self.run_context(source, "parse", collection_id);
        source
            .implementation
            .parse_artifact_runtime(collection_id, artifact, &ctx)
    }

    pub fn stream_structured_parse(
        &self,
        source_id: &str,
        artifact: &LocalArtifact,
        collection_id: &str,
        sink: &mut dyn ingest_core::RawTableRowSink,
    ) -> Result<()> {
        let source = self.source(source_id)?;
        let ctx = self.run_context(source, "parse", collection_id);
        source
            .implementation
            .stream_structured_parse_runtime(artifact, collection_id, &ctx, sink)
    }

    fn register<P>(&mut self, plugin: P)
    where
        P: RuntimeSourcePlugin + 'static,
    {
        let implementation: Arc<dyn RuntimeSourcePlugin> = Arc::new(plugin);
        let plan = SourcePlan {
            descriptor: implementation.descriptor(),
            collections: implementation.collections(),
            metadata_documents: implementation.metadata_catalog(),
        };
        let source_id = plan.descriptor.source_id.clone();
        let parser_version = implementation.parser_version().to_string();
        let index = self.sources.len();
        self.source_indexes.insert(source_id, index);
        self.sources.push(RegisteredSource {
            parser_version,
            plan,
            implementation,
        });
    }

    fn source(&self, source_id: &str) -> Result<&RegisteredSource> {
        let index = self
            .source_indexes
            .get(source_id)
            .copied()
            .ok_or_else(|| anyhow!("no implementation registered for source '{source_id}'"))?;
        Ok(&self.sources[index])
    }

    fn run_context(
        &self,
        source: &RegisteredSource,
        prefix: &str,
        collection_id: &str,
    ) -> RunContext {
        RunContext {
            run_id: format!("{prefix}-{collection_id}-{}", Utc::now().timestamp_millis()),
            environment: "service".to_string(),
            parser_version: source.parser_version.clone(),
        }
    }
}

pub fn build_source_plans() -> Vec<SourcePlan> {
    SourceRegistry::new().source_plans()
}

pub fn build_schedule_seeds(source_plans: &[SourcePlan]) -> Vec<CollectionScheduleSeed> {
    source_plans
        .iter()
        .flat_map(|source| {
            source
                .collections
                .iter()
                .map(|collection| CollectionScheduleSeed {
                    source_id: source.descriptor.source_id.clone(),
                    collection_id: collection.id.clone(),
                    poll_interval_seconds: collection.default_poll_interval_seconds.unwrap_or(300),
                    stagger_offset_seconds: stable_stagger_offset_seconds(
                        &source.descriptor.source_id,
                        &collection.id,
                        collection.default_poll_interval_seconds.unwrap_or(300),
                    ),
                    scheduler_enabled: true,
                })
        })
        .collect()
}
