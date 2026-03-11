pub mod catalog;
pub mod discovery;
pub mod fetch;
pub mod mms_parser;
pub mod parse;
pub mod seed_concepts;
pub mod seed_reference;

use std::path::Path;

use anyhow::{Result, bail};
use ingest_core::{
    CollectionCompletion, CompletionUnit, DiscoveredArtifact, DiscoveryRequest, LocalArtifact,
    ParseResult, PluginCapabilities, PromotionSpec, RawPluginParseResult, RawTableRowSink,
    RunContext, SourceCollection, SourceDescriptor, SourceMetadataDocument, SourcePlugin,
    TaskBlueprint, TaskKind,
};

use crate::catalog::AemoCatalog;

#[derive(Clone)]
pub struct AemoMetadataHtmlPlugin;

impl AemoMetadataHtmlPlugin {
    pub fn new() -> Self {
        Self
    }

    pub async fn discover_collection(
        &self,
        client: &reqwest::Client,
        collection_id: &str,
        ctx: &RunContext,
    ) -> Result<Vec<DiscoveredArtifact>> {
        discovery::discover_collection(client, collection_id, ctx).await
    }

    pub async fn fetch_artifact(
        &self,
        client: &reqwest::Client,
        artifact: &DiscoveredArtifact,
        output_dir: &Path,
    ) -> Result<LocalArtifact> {
        fetch::fetch_artifact(client, artifact, output_dir).await
    }

    pub fn parse_artifact(
        &self,
        collection_id: &str,
        artifact: &LocalArtifact,
    ) -> Result<RawPluginParseResult> {
        parse::parse_artifact(collection_id, artifact)
    }

    pub async fn build_catalog(&self, client: &reqwest::Client) -> Result<AemoCatalog> {
        let (packages, tables) = mms_parser::fetch_full_model(client).await?;

        Ok(AemoCatalog {
            packages,
            tables,
            concepts: seed_concepts::seed_concepts(),
            fcas_markets: seed_reference::seed_fcas_markets(),
            interconnectors: seed_reference::seed_interconnectors(),
        })
    }

    pub fn catalog_to_json(catalog: &AemoCatalog) -> Result<String> {
        Ok(serde_json::to_string_pretty(catalog)?)
    }
}

impl Default for AemoMetadataHtmlPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl SourcePlugin for AemoMetadataHtmlPlugin {
    fn descriptor(&self) -> SourceDescriptor {
        SourceDescriptor {
            source_id: "aemo_metadata_html".to_string(),
            domain: "metadata".to_string(),
            description: "AEMO HTML metadata supplements for current model and reference pages."
                .to_string(),
            versioned_metadata: true,
            historical_backfill_supported: true,
        }
    }

    fn capabilities(&self) -> PluginCapabilities {
        PluginCapabilities {
            supports_backfill: true,
            supports_schema_registry: false,
            supports_historical_media: true,
            notes: vec![
                "Discovers metadata documents dynamically from live AEMO index pages.".to_string(),
                "Extracts raw HTML metadata tables for current explanations and mappings."
                    .to_string(),
            ],
        }
    }

    fn collections(&self) -> Vec<SourceCollection> {
        vec![
            SourceCollection {
                id: "current-data-model".to_string(),
                display_name: "Current Data Model".to_string(),
                description: "Current HTML model bundle plus index page references.".to_string(),
                retrieval_modes: vec!["discover-index".to_string(), "fetch-html".to_string()],
                completion: CollectionCompletion {
                    unit: CompletionUnit::DocumentVersion,
                    dedupe_keys: vec!["artifact_id".to_string(), "content_sha256".to_string()],
                    cursor_field: Some("model_version".to_string()),
                    mutable_window_seconds: Some(86_400),
                    notes: vec![
                        "Current HTML bundle is keyed by discovered model version.".to_string(),
                        "HTML is supplemental to DVD DDL for core table comments.".to_string(),
                    ],
                },
                task_blueprints: metadata_task_blueprints(),
                default_poll_interval_seconds: Some(86_400),
            },
            SourceCollection {
                id: "report-relationships".to_string(),
                display_name: "Table Report Relationships".to_string(),
                description: "Daily snapshot of the AEMO table-to-report relationship page."
                    .to_string(),
                retrieval_modes: vec!["fetch-html".to_string(), "fetch-csv-export".to_string()],
                completion: CollectionCompletion {
                    unit: CompletionUnit::DocumentVersion,
                    dedupe_keys: vec!["artifact_id".to_string(), "content_sha256".to_string()],
                    cursor_field: None,
                    mutable_window_seconds: Some(86_400),
                    notes: vec![
                        "Stable URLs are treated as daily snapshots because content can change."
                            .to_string(),
                    ],
                },
                task_blueprints: metadata_task_blueprints(),
                default_poll_interval_seconds: Some(86_400),
            },
            SourceCollection {
                id: "population-dates".to_string(),
                display_name: "Population Dates".to_string(),
                description: "Daily snapshot of the AEMO population dates page.".to_string(),
                retrieval_modes: vec!["fetch-html".to_string()],
                completion: CollectionCompletion {
                    unit: CompletionUnit::DocumentVersion,
                    dedupe_keys: vec!["artifact_id".to_string(), "content_sha256".to_string()],
                    cursor_field: None,
                    mutable_window_seconds: Some(86_400),
                    notes: vec![
                        "Population-date changes are historised as append-only metadata objects."
                            .to_string(),
                    ],
                },
                task_blueprints: metadata_task_blueprints(),
                default_poll_interval_seconds: Some(86_400),
            },
        ]
    }

    fn metadata_catalog(&self) -> Vec<SourceMetadataDocument> {
        Vec::new()
    }

    fn discover(
        &self,
        _request: &DiscoveryRequest,
        _ctx: &RunContext,
    ) -> Result<Vec<DiscoveredArtifact>> {
        bail!("Use discover_collection() for async metadata discovery")
    }

    fn fetch(&self, _artifact: &DiscoveredArtifact, _ctx: &RunContext) -> Result<LocalArtifact> {
        bail!("Use fetch_artifact() for async metadata fetching")
    }

    fn inspect_parse(&self, _artifact: &LocalArtifact, _ctx: &RunContext) -> Result<ParseResult> {
        bail!(
            "Metadata parsing is published into plugin-owned raw tables, not schema-hash raw tables"
        )
    }

    fn stream_parse(
        &self,
        _artifact: &LocalArtifact,
        _ctx: &RunContext,
        _sink: &mut dyn RawTableRowSink,
    ) -> Result<()> {
        bail!(
            "Metadata parsing is published into plugin-owned raw tables, not schema-hash raw tables"
        )
    }

    fn promotion_plan(&self) -> &'static [PromotionSpec] {
        &[]
    }
}

fn metadata_task_blueprints() -> Vec<TaskBlueprint> {
    vec![
        TaskBlueprint {
            kind: TaskKind::Discover,
            description: "Discover live metadata documents and daily snapshots.".to_string(),
            max_concurrency: 1,
            queue: "metadata-discover".to_string(),
            idempotency_scope: "source+collection+artifact".to_string(),
        },
        TaskBlueprint {
            kind: TaskKind::Fetch,
            description: "Fetch HTML metadata documents and current model bundle.".to_string(),
            max_concurrency: 2,
            queue: "metadata-fetch".to_string(),
            idempotency_scope: "artifact_id".to_string(),
        },
        TaskBlueprint {
            kind: TaskKind::Parse,
            description: "Extract historised metadata objects from fetched artifacts.".to_string(),
            max_concurrency: 2,
            queue: "metadata-parse".to_string(),
            idempotency_scope: "artifact_id+parser_version".to_string(),
        },
    ]
}
