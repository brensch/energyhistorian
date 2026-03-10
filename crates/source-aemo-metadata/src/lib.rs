use anyhow::{Result, bail};
use chrono::Utc;
use ingest_core::{
    ArtifactKind, ArtifactMetadata, CollectionCompletion, CompletionUnit, DiscoveredArtifact,
    DiscoveryRequest, LocalArtifact, ParseResult, PluginCapabilities, PromotionSpec,
    RawTableRowSink, RunContext, SourceCollection, SourceDescriptor, SourceMetadataDocument,
    SourcePlugin, TaskBlueprint, TaskKind,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataDocument {
    pub model_version: Option<&'static str>,
    pub release_name: &'static str,
    pub document_type: &'static str,
    pub url: &'static str,
    pub notes: &'static str,
}

pub struct AemoMetadataPlugin;

impl AemoMetadataPlugin {
    pub fn new() -> Self {
        Self
    }

    pub fn catalog(&self) -> &'static [MetadataDocument] {
        METADATA_DOCUMENTS
    }
}

impl Default for AemoMetadataPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl SourcePlugin for AemoMetadataPlugin {
    fn descriptor(&self) -> SourceDescriptor {
        SourceDescriptor {
            source_id: "aemo.metadata".to_string(),
            domain: "metadata".to_string(),
            description: "AEMO versioned Data Model, report-mapping, and release metadata."
                .to_string(),
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
                "Supports versioned Data Model report ingestion across multiple AEMO releases."
                    .to_string(),
                "Should normalize table definitions, report mappings, upgrade notes, and population dates."
                    .to_string(),
            ],
        }
    }

    fn collections(&self) -> Vec<SourceCollection> {
        vec![
            SourceCollection {
                id: "data-model-reports".to_string(),
                display_name: "Data Model Reports".to_string(),
                description: "Versioned Electricity Data Model report documents.".to_string(),
                retrieval_modes: vec!["fetch-html".to_string(), "fetch-pdf".to_string()],
                completion: CollectionCompletion {
                    unit: CompletionUnit::DocumentVersion,
                    dedupe_keys: vec!["url".to_string(), "model_version".to_string()],
                    cursor_field: Some("model_version".to_string()),
                    mutable_window_seconds: None,
                    notes: vec![
                        "Completion is tracked per metadata document release.".to_string(),
                        "Older model versions must remain available for historical parsing."
                            .to_string(),
                    ],
                },
                task_blueprints: vec![
                    TaskBlueprint {
                        kind: TaskKind::Discover,
                        description: "Enumerate known and newly published metadata documents."
                            .to_string(),
                        max_concurrency: 1,
                        queue: "metadata-discover".to_string(),
                        idempotency_scope: "source+collection+url".to_string(),
                    },
                    TaskBlueprint {
                        kind: TaskKind::Fetch,
                        description: "Download authoritative metadata documents.".to_string(),
                        max_concurrency: 2,
                        queue: "metadata-fetch".to_string(),
                        idempotency_scope: "artifact_id".to_string(),
                    },
                    TaskBlueprint {
                        kind: TaskKind::Parse,
                        description: "Normalize model tables, columns, and release metadata."
                            .to_string(),
                        max_concurrency: 2,
                        queue: "metadata-parse".to_string(),
                        idempotency_scope: "artifact_id+parser_version".to_string(),
                    },
                    TaskBlueprint {
                        kind: TaskKind::SyncMetadata,
                        description: "Update the control-plane metadata registry.".to_string(),
                        max_concurrency: 1,
                        queue: "metadata-sync".to_string(),
                        idempotency_scope: "model_version+document_type".to_string(),
                    },
                ],
                default_poll_interval_seconds: Some(86_400),
            },
            SourceCollection {
                id: "report-relationships".to_string(),
                display_name: "Table Report Relationships".to_string(),
                description: "Maps file/report IDs to Data Model tables.".to_string(),
                retrieval_modes: vec!["fetch-html".to_string(), "fetch-csv-export".to_string()],
                completion: CollectionCompletion {
                    unit: CompletionUnit::DocumentVersion,
                    dedupe_keys: vec!["url".to_string()],
                    cursor_field: None,
                    mutable_window_seconds: Some(86_400),
                    notes: vec![
                        "Relationship documents should be versioned even when the URL is stable."
                            .to_string(),
                    ],
                },
                task_blueprints: vec![
                    TaskBlueprint {
                        kind: TaskKind::Discover,
                        description: "Check relationship documents for updates.".to_string(),
                        max_concurrency: 1,
                        queue: "metadata-discover".to_string(),
                        idempotency_scope: "source+collection+url".to_string(),
                    },
                    TaskBlueprint {
                        kind: TaskKind::Fetch,
                        description: "Fetch current relationship document variants.".to_string(),
                        max_concurrency: 1,
                        queue: "metadata-fetch".to_string(),
                        idempotency_scope: "artifact_id".to_string(),
                    },
                    TaskBlueprint {
                        kind: TaskKind::Parse,
                        description: "Normalize table-to-report mappings.".to_string(),
                        max_concurrency: 1,
                        queue: "metadata-parse".to_string(),
                        idempotency_scope: "artifact_id+parser_version".to_string(),
                    },
                    TaskBlueprint {
                        kind: TaskKind::SyncMetadata,
                        description: "Refresh collection-to-logical-table mappings.".to_string(),
                        max_concurrency: 1,
                        queue: "metadata-sync".to_string(),
                        idempotency_scope: "collection".to_string(),
                    },
                ],
                default_poll_interval_seconds: Some(86_400),
            },
            SourceCollection {
                id: "population-dates".to_string(),
                display_name: "Population Dates".to_string(),
                description: "Operational start dates for tables and file IDs.".to_string(),
                retrieval_modes: vec!["fetch-html".to_string()],
                completion: CollectionCompletion {
                    unit: CompletionUnit::DocumentVersion,
                    dedupe_keys: vec!["url".to_string()],
                    cursor_field: None,
                    mutable_window_seconds: Some(86_400),
                    notes: vec!["Population dates may be revised after publication.".to_string()],
                },
                task_blueprints: vec![
                    TaskBlueprint {
                        kind: TaskKind::Discover,
                        description: "Check population date documents for updates.".to_string(),
                        max_concurrency: 1,
                        queue: "metadata-discover".to_string(),
                        idempotency_scope: "source+collection+url".to_string(),
                    },
                    TaskBlueprint {
                        kind: TaskKind::Fetch,
                        description: "Fetch current population date documents.".to_string(),
                        max_concurrency: 1,
                        queue: "metadata-fetch".to_string(),
                        idempotency_scope: "artifact_id".to_string(),
                    },
                    TaskBlueprint {
                        kind: TaskKind::Parse,
                        description: "Normalize table start-date metadata.".to_string(),
                        max_concurrency: 1,
                        queue: "metadata-parse".to_string(),
                        idempotency_scope: "artifact_id+parser_version".to_string(),
                    },
                    TaskBlueprint {
                        kind: TaskKind::SyncMetadata,
                        description: "Refresh table availability hints for backfill planning."
                            .to_string(),
                        max_concurrency: 1,
                        queue: "metadata-sync".to_string(),
                        idempotency_scope: "collection".to_string(),
                    },
                ],
                default_poll_interval_seconds: Some(86_400),
            },
        ]
    }

    fn metadata_catalog(&self) -> Vec<SourceMetadataDocument> {
        self.catalog()
            .iter()
            .enumerate()
            .map(|(idx, doc)| SourceMetadataDocument {
                id: format!("aemo-metadata-{}", idx),
                title: doc.release_name.to_string(),
                version: doc.model_version.map(str::to_string),
                document_type: doc.document_type.to_string(),
                url: doc.url.to_string(),
                notes: doc.notes.to_string(),
            })
            .collect()
    }

    fn discover(
        &self,
        _request: &DiscoveryRequest,
        ctx: &RunContext,
    ) -> Result<Vec<DiscoveredArtifact>> {
        Ok(self
            .catalog()
            .iter()
            .enumerate()
            .map(|(idx, doc)| DiscoveredArtifact {
                metadata: ArtifactMetadata {
                    artifact_id: format!("{}-{}-{}", self.descriptor().source_id, ctx.run_id, idx),
                    source_id: self.descriptor().source_id.clone(),
                    acquisition_uri: doc.url.to_string(),
                    discovered_at: Utc::now(),
                    fetched_at: None,
                    published_at: None,
                    content_sha256: None,
                    content_length_bytes: None,
                    kind: infer_kind(doc.document_type),
                    parser_version: ctx.parser_version.clone(),
                    model_version: doc.model_version.map(str::to_string),
                    release_name: Some(doc.release_name.to_string()),
                },
            })
            .collect())
    }

    fn fetch(&self, _artifact: &DiscoveredArtifact, _ctx: &RunContext) -> Result<LocalArtifact> {
        bail!("Metadata fetch/parser implementation still needs to be built")
    }

    fn inspect_parse(&self, _artifact: &LocalArtifact, _ctx: &RunContext) -> Result<ParseResult> {
        bail!("Metadata normalization is the next major implementation step")
    }

    fn stream_parse(
        &self,
        _artifact: &LocalArtifact,
        _ctx: &RunContext,
        _sink: &mut dyn RawTableRowSink,
    ) -> Result<()> {
        bail!("Metadata normalization should stream rows through the shared ingestion contract")
    }

    fn promotion_plan(&self) -> &'static [PromotionSpec] {
        &[]
    }
}

fn infer_kind(document_type: &str) -> ArtifactKind {
    match document_type {
        "pdf" => ArtifactKind::PdfDocument,
        "html" => ArtifactKind::HtmlDocument,
        "csv" => ArtifactKind::CsvExport,
        _ => ArtifactKind::Unknown,
    }
}

const METADATA_DOCUMENTS: &[MetadataDocument] = &[
    MetadataDocument {
        model_version: Some("5.6"),
        release_name: "EMMS DM 5.6 November 2025",
        document_type: "pdf",
        url: "https://tech-specs.docs.public.aemo.com.au/Content/TSP_EMMSDM56_Nov2025/EMMS_DM5.6_Nov_2025_v3.01_Markup.pdf",
        notes: "Current detailed Electricity Data Model report.",
    },
    MetadataDocument {
        model_version: Some("5.6"),
        release_name: "EMMS DM 5.6 November 2025",
        document_type: "html",
        url: "https://tech-specs.docs.public.aemo.com.au/Content/TSP_EMMSDM56_Nov2025/EMMS_-_DM5.6_-_Nov_2025_1.htm",
        notes: "Release landing page and upgrade context for DM 5.6.",
    },
    MetadataDocument {
        model_version: Some("5.5"),
        release_name: "EMMS DM 5.5 April 2025",
        document_type: "html",
        url: "https://tech-specs.docs.public.aemo.com.au/Content/TSP_EMMSDM55_Apr2025/EMMS_DMv5.5_Apr25_1.htm",
        notes: "Prior version useful for schema and release comparisons.",
    },
    MetadataDocument {
        model_version: Some("5.3"),
        release_name: "EMMS DM 5.3 April 2024",
        document_type: "html",
        url: "https://tech-specs.docs.public.aemo.com.au/Content/TSP_EMMSDM53_April2024/Participant_Impact.htm",
        notes: "Participant impact page with report migrations and replacements.",
    },
    MetadataDocument {
        model_version: Some("5.2"),
        release_name: "EMMS DM 5.2 May 2023",
        document_type: "html",
        url: "https://tech-specs.docs.public.aemo.com.au/Content/TSP_EMMSDM52May2023/Introduction.htm",
        notes: "Older release line for historical compatibility.",
    },
    MetadataDocument {
        model_version: None,
        release_name: "Table to Report Relationships",
        document_type: "html",
        url: "https://di-help.docs.public.aemo.com.au/Content/Data_Subscription/TableToFileReport.htm",
        notes: "Maps file/report IDs to logical Data Model tables.",
    },
    MetadataDocument {
        model_version: None,
        release_name: "Data Population Dates",
        document_type: "html",
        url: "https://tech-specs.docs.public.aemo.com.au/Content/TSP_TechnicalSpecificationPortal/Data_population_dates.htm",
        notes: "Operational start dates for tables and file IDs.",
    },
    MetadataDocument {
        model_version: None,
        release_name: "Data Model Reports Index",
        document_type: "html",
        url: "https://di-help.docs.public.aemo.com.au/Content/Data_Model/MMS_Data_Model.htm?TocPath=_____8",
        notes: "Index of Data Model reports and related package summaries.",
    },
    MetadataDocument {
        model_version: Some("legacy"),
        release_name: "EMMS Technical Specification October to December 2017",
        document_type: "pdf",
        url: "https://aemo.com.au/-/media/Files/Electricity/NEM/IT-Systems-and-Change/2017/EMMS-Technical-Specification---October-to-December-2017.pdf",
        notes: "Legacy technical specification for older historical structures.",
    },
];
