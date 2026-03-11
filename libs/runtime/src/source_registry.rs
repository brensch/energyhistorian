use std::path::Path;

use anyhow::{Result, anyhow, bail};
use chrono::Utc;
use ingest_core::{
    DiscoveredArtifact, LocalArtifact, RawPluginParseResult, RunContext, SourceCollection,
    SourceDescriptor, SourceMetadataDocument, SourcePlugin,
};
use serde::Serialize;
use source_aemo_dvd::AemoMetadataDvdPlugin;
use source_aemo_metadata::AemoMetadataHtmlPlugin;
use source_nemweb::NemwebPlugin;

use crate::queue::{CollectionScheduleSeed, stable_stagger_offset_seconds};

#[derive(Debug, Clone, Serialize)]
pub struct SourcePlan {
    pub descriptor: SourceDescriptor,
    pub collections: Vec<SourceCollection>,
    pub metadata_documents: Vec<SourceMetadataDocument>,
}

#[derive(Clone)]
pub struct SourceRegistry {
    nemweb: NemwebPlugin,
    metadata_html: AemoMetadataHtmlPlugin,
    metadata_dvd: AemoMetadataDvdPlugin,
}

#[derive(Debug)]
pub enum ParsedArtifact {
    Nemweb {
        artifact: LocalArtifact,
        result: ingest_core::ParseResult,
    },
    RawMetadata {
        artifact: LocalArtifact,
        result: RawPluginParseResult,
    },
}

impl Default for SourceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl SourceRegistry {
    pub fn new() -> Self {
        Self {
            nemweb: NemwebPlugin::new(),
            metadata_html: AemoMetadataHtmlPlugin::new(),
            metadata_dvd: AemoMetadataDvdPlugin::new(),
        }
    }

    pub fn source_plans(&self) -> Vec<SourcePlan> {
        vec![
            SourcePlan {
                descriptor: self.nemweb.descriptor(),
                collections: self.nemweb.collections(),
                metadata_documents: self.nemweb.metadata_catalog(),
            },
            SourcePlan {
                descriptor: self.metadata_html.descriptor(),
                collections: self.metadata_html.collections(),
                metadata_documents: self.metadata_html.metadata_catalog(),
            },
            SourcePlan {
                descriptor: self.metadata_dvd.descriptor(),
                collections: self.metadata_dvd.collections(),
                metadata_documents: self.metadata_dvd.metadata_catalog(),
            },
        ]
    }

    pub async fn discover(
        &self,
        client: &reqwest::Client,
        source_id: &str,
        collection_id: &str,
        limit: usize,
    ) -> Result<Vec<DiscoveredArtifact>> {
        match source_id {
            "aemo.nemweb" => {
                self.nemweb
                    .discover_collection(client, collection_id, limit)
                    .await
            }
            "aemo_metadata_html" => {
                let ctx = run_context("discover", collection_id, "source-aemo-metadata/0.1");
                self.metadata_html
                    .discover_collection(client, collection_id, &ctx)
                    .await
            }
            "aemo_metadata_dvd" => {
                let ctx = run_context("discover", collection_id, "source-aemo-dvd/0.1");
                self.metadata_dvd
                    .discover_collection(client, collection_id, &ctx)
                    .await
            }
            _ => bail!("no discover implementation for source '{source_id}'"),
        }
    }

    pub async fn fetch(
        &self,
        client: &reqwest::Client,
        source_id: &str,
        _collection_id: &str,
        artifact: &DiscoveredArtifact,
        output_dir: &Path,
    ) -> Result<LocalArtifact> {
        match source_id {
            "aemo.nemweb" => {
                self.nemweb
                    .fetch_artifact(client, artifact, output_dir)
                    .await
            }
            "aemo_metadata_html" => {
                self.metadata_html
                    .fetch_artifact(client, artifact, output_dir)
                    .await
            }
            "aemo_metadata_dvd" => {
                self.metadata_dvd
                    .fetch_artifact(client, artifact, output_dir)
                    .await
            }
            _ => bail!("no fetch implementation for source '{source_id}'"),
        }
    }

    pub fn parse(
        &self,
        source_id: &str,
        collection_id: &str,
        artifact: LocalArtifact,
    ) -> Result<ParsedArtifact> {
        let ctx = match source_id {
            "aemo_metadata_html" => run_context("parse", collection_id, "source-aemo-metadata/0.1"),
            "aemo_metadata_dvd" => run_context("parse", collection_id, "source-aemo-dvd/0.1"),
            "aemo.nemweb" => run_context("parse", collection_id, "source-nemweb/0.1"),
            _ => return Err(anyhow!("no parse implementation for source '{source_id}'")),
        };

        match source_id {
            "aemo.nemweb" => {
                let result = self
                    .nemweb
                    .inspect_parse(&artifact, &ctx)
                    .map_err(|error| anyhow!("inspecting nemweb artifact: {error:#}"))?;
                Ok(ParsedArtifact::Nemweb { artifact, result })
            }
            "aemo_metadata_html" => {
                let result = self
                    .metadata_html
                    .parse_artifact(collection_id, &artifact)
                    .map_err(|error| anyhow!("parsing html metadata artifact: {error:#}"))?;
                Ok(ParsedArtifact::RawMetadata { artifact, result })
            }
            "aemo_metadata_dvd" => {
                let result = self
                    .metadata_dvd
                    .parse_artifact(&artifact)
                    .map_err(|error| anyhow!("parsing dvd metadata artifact: {error:#}"))?;
                Ok(ParsedArtifact::RawMetadata { artifact, result })
            }
            _ => bail!("no parse implementation for source '{source_id}'"),
        }
    }

    pub fn stream_nemweb_parse(
        &self,
        artifact: &LocalArtifact,
        collection_id: &str,
        sink: &mut dyn ingest_core::RawTableRowSink,
    ) -> Result<()> {
        let ctx = run_context("parse", collection_id, "source-nemweb/0.1");
        self.nemweb.stream_parse(artifact, &ctx, sink)
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

fn run_context(prefix: &str, collection_id: &str, parser_version: &str) -> RunContext {
    RunContext {
        run_id: format!("{prefix}-{collection_id}-{}", Utc::now().timestamp_millis()),
        environment: "service".to_string(),
        parser_version: parser_version.to_string(),
    }
}
