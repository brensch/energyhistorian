use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use chrono::Utc;
use ingest_core::{
    DiscoveredArtifact, DiscoveryCursorHint, LocalArtifact, RunContext, RuntimePluginParseResult,
    RuntimeSourcePlugin, SemanticJob, SourceCollection, SourceDescriptor, SourceMetadataDocument,
    StructuredRawEventSink,
};
use serde::Serialize;
use source_aemo_docs::AemoDocsPlugin;
use source_mmsdm_data::MmsdmDataPlugin;
use source_mmsdm_meta::MmsdmMetaPlugin;
use source_nemweb_data::NemwebDataPlugin;

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

#[derive(Debug, Clone)]
pub struct ScheduleSeed {
    pub source_id: String,
    pub collection_id: String,
    pub poll_interval_seconds: u64,
    pub stagger_offset_seconds: u64,
    pub parser_version: String,
}

impl SourceRegistry {
    pub fn new() -> Self {
        let mut registry = Self {
            sources: Vec::new(),
            source_indexes: HashMap::new(),
        };
        registry.register(NemwebDataPlugin::new());
        registry.register(MmsdmDataPlugin::new());
        registry.register(AemoDocsPlugin::new());
        registry.register(MmsdmMetaPlugin::new());
        registry
    }

    pub fn schedule_seeds(&self) -> Vec<ScheduleSeed> {
        self.sources
            .iter()
            .flat_map(|source| {
                source.plan.collections.iter().map(|collection| {
                    let poll = collection.default_poll_interval_seconds.unwrap_or(300);
                    ScheduleSeed {
                        source_id: source.plan.descriptor.source_id.clone(),
                        collection_id: collection.id.clone(),
                        poll_interval_seconds: poll,
                        stagger_offset_seconds: stable_stagger_offset(
                            &source.plan.descriptor.source_id,
                            &collection.id,
                            poll,
                        ),
                        parser_version: source.parser_version.clone(),
                    }
                })
            })
            .collect()
    }

    pub fn source_plan(&self, source_id: &str) -> Result<SourcePlan> {
        Ok(self.source(source_id)?.plan.clone())
    }

    pub fn filter_to(mut self, source_id: &str, collection_id: Option<&str>) -> Result<Self> {
        let mut selected = self.source(source_id)?.clone();
        if let Some(collection_id) = collection_id {
            selected
                .plan
                .collections
                .retain(|collection| collection.id == collection_id);
            if selected.plan.collections.is_empty() {
                return Err(anyhow!(
                    "source '{source_id}' has no collection '{collection_id}'"
                ));
            }
        }
        self.sources = vec![selected];
        self.source_indexes.clear();
        self.source_indexes.insert(source_id.to_string(), 0);
        Ok(self)
    }

    pub async fn discover(
        &self,
        client: &reqwest::Client,
        source_id: &str,
        collection_id: &str,
        limit: usize,
        cursor: &DiscoveryCursorHint,
    ) -> Result<Vec<DiscoveredArtifact>> {
        let source = self.source(source_id)?;
        let ctx = self.run_context(source, "discover", collection_id);
        source
            .implementation
            .discover_collection_async(client, collection_id, limit, cursor, &ctx)
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

    pub fn stream_structured_parse_events(
        &self,
        source_id: &str,
        artifact: &LocalArtifact,
        collection_id: &str,
        sink: &mut dyn StructuredRawEventSink,
    ) -> Result<()> {
        let source = self.source(source_id)?;
        let ctx = self.run_context(source, "parse", collection_id);
        source
            .implementation
            .stream_structured_parse_events_runtime(artifact, collection_id, &ctx, sink)
    }

    pub fn semantic_jobs(&self, source_id: &str) -> Result<Vec<SemanticJob>> {
        let source = self.source(source_id)?;
        Ok(source.implementation.semantic_jobs())
    }

    pub fn parser_version(&self, source_id: &str) -> Result<String> {
        let source = self.source(source_id)?;
        Ok(source.parser_version.clone())
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

    #[cfg(test)]
    pub(crate) fn from_plugin_for_test<P>(plugin: P) -> Self
    where
        P: RuntimeSourcePlugin + 'static,
    {
        let mut registry = Self {
            sources: Vec::new(),
            source_indexes: HashMap::new(),
        };
        registry.register(plugin);
        registry
    }
}

pub(crate) fn stable_stagger_offset(
    source_id: &str,
    collection_id: &str,
    poll_interval_seconds: u64,
) -> u64 {
    if poll_interval_seconds <= 1 {
        return 0;
    }
    let mut hasher = DefaultHasher::new();
    source_id.hash(&mut hasher);
    collection_id.hash(&mut hasher);
    hasher.finish() % poll_interval_seconds
}

#[cfg(test)]
mod tests {
    use super::SourceRegistry;

    #[test]
    fn registers_all_renamed_source_plugins() {
        let registry = SourceRegistry::new();
        for source_id in [
            "aemo.nemweb.data",
            "aemo.mmsdm.data",
            "aemo.docs",
            "aemo.mmsdm.meta",
        ] {
            registry
                .source_plan(source_id)
                .unwrap_or_else(|err| panic!("missing source '{source_id}': {err}"));
        }
    }
}
