use anyhow::{Result, bail};
use chrono::Utc;
use ingest_core::{
    ArtifactKind, ArtifactMetadata, CollectionCompletion, CompletionUnit, DiscoveredArtifact,
    DiscoveryRequest, LocalArtifact, ParseResult, PluginCapabilities, PromotionSpec, RunContext,
    SourceCollection, SourceDescriptor, SourceMetadataDocument, SourcePlugin, TaskBlueprint,
    TaskKind,
};

pub struct AemoDvdPlugin;

impl AemoDvdPlugin {
    pub fn new() -> Self {
        Self
    }
}

impl SourcePlugin for AemoDvdPlugin {
    fn descriptor(&self) -> SourceDescriptor {
        SourceDescriptor {
            source_id: "aemo.dvd".to_string(),
            domain: "historical-media".to_string(),
            description: "AEMO historical DVD/media distributions for deep backfill.".to_string(),
            versioned_metadata: true,
            historical_backfill_supported: true,
        }
    }

    fn capabilities(&self) -> PluginCapabilities {
        PluginCapabilities {
            supports_backfill: true,
            supports_schema_registry: true,
            supports_historical_media: true,
            notes: vec![
                "Intended for deep historical backfill from monthly/annual media.".to_string(),
                "Needs media acquisition and extraction workflows separate from live NEMweb."
                    .to_string(),
            ],
        }
    }

    fn collections(&self) -> Vec<SourceCollection> {
        vec![SourceCollection {
            id: "dvd-subscriptions".to_string(),
            display_name: "DVD Historical Media".to_string(),
            description: "Monthly or annual historical media deliveries.".to_string(),
            retrieval_modes: vec!["acquire-media".to_string(), "extract-image".to_string()],
            completion: CollectionCompletion {
                unit: CompletionUnit::MediaImage,
                dedupe_keys: vec![
                    "release_name".to_string(),
                    "published_at".to_string(),
                    "content_sha256".to_string(),
                ],
                cursor_field: Some("published_at".to_string()),
                mutable_window_seconds: None,
                notes: vec![
                    "Completion is tracked per acquired media image or extracted payload."
                        .to_string(),
                ],
            },
            task_blueprints: vec![
                TaskBlueprint {
                    kind: TaskKind::Discover,
                    description: "Enumerate available historical media batches.".to_string(),
                    max_concurrency: 1,
                    queue: "media-discover".to_string(),
                    idempotency_scope: "source+collection+release".to_string(),
                },
                TaskBlueprint {
                    kind: TaskKind::Fetch,
                    description: "Acquire media images or subscription drops.".to_string(),
                    max_concurrency: 1,
                    queue: "media-fetch".to_string(),
                    idempotency_scope: "artifact_id".to_string(),
                },
                TaskBlueprint {
                    kind: TaskKind::Parse,
                    description: "Extract files from historical media and normalize contents."
                        .to_string(),
                    max_concurrency: 1,
                    queue: "media-parse".to_string(),
                    idempotency_scope: "artifact_id+parser_version".to_string(),
                },
            ],
            default_poll_interval_seconds: None,
        }]
    }

    fn metadata_catalog(&self) -> Vec<SourceMetadataDocument> {
        Vec::new()
    }

    fn discover(
        &self,
        _request: &DiscoveryRequest,
        ctx: &RunContext,
    ) -> Result<Vec<DiscoveredArtifact>> {
        Ok(vec![DiscoveredArtifact {
            metadata: ArtifactMetadata {
                artifact_id: format!("{}-dvd-placeholder", ctx.run_id),
                source_id: self.descriptor().source_id.clone(),
                acquisition_uri: "subscription://aemo/historical-dvd".to_string(),
                discovered_at: Utc::now(),
                fetched_at: None,
                published_at: None,
                content_sha256: None,
                content_length_bytes: None,
                kind: ArtifactKind::DvdImage,
                parser_version: ctx.parser_version.clone(),
                model_version: None,
                release_name: None,
            },
        }])
    }

    fn fetch(&self, _artifact: &DiscoveredArtifact, _ctx: &RunContext) -> Result<LocalArtifact> {
        bail!("DVD acquisition/extraction implementation still needs to be built")
    }

    fn parse(&self, _artifact: &LocalArtifact, _ctx: &RunContext) -> Result<ParseResult> {
        bail!("DVD parsing should be built on top of the same schema registry as NEMweb")
    }

    fn promotion_plan(&self) -> &'static [PromotionSpec] {
        &[]
    }
}
