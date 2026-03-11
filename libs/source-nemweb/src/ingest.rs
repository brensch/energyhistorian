use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::Utc;
use ingest_core::{ObservedSchema, RunContext};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::discover::discover_recent_archives;
use crate::families::lookup_family;
use crate::fetch::fetch_archive;
use crate::parse::{parse_local_archive, sanitize_logical_table_key};

const USER_AGENT: &str = "energyhistorian/0.1 (+https://github.com/brensch/energyhistorian)";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveManifest {
    pub artifact_id: String,
    pub archive_name: String,
    pub archive_path: String,
    pub source_url: String,
    pub sha256: String,
    pub bytes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParsedTableBatch {
    pub logical_table_key: String,
    pub output_name: String,
    pub output_path: String,
    pub rows_written: usize,
    pub batch_jsonl: String,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NemwebIngestResult {
    pub source_family: String,
    pub source_url: String,
    pub fetched_at_utc: String,
    pub archives: Vec<ArchiveManifest>,
    pub tables: Vec<ParsedTableBatch>,
    pub observed_schemas: Vec<ObservedSchema>,
}

pub async fn ingest_recent(
    source_family_id: &str,
    limit: usize,
    raw_dir: &Path,
    parsed_dir: &Path,
) -> Result<NemwebIngestResult> {
    let family = lookup_family(source_family_id)?;
    let ctx = RunContext {
        run_id: format!("{}-{}", family.id, Utc::now().timestamp_millis()),
        environment: "local".to_string(),
        parser_version: "source-nemweb/0.1".to_string(),
    };
    let client = reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .build()
        .context("building HTTP client")?;

    let discovered = discover_recent_archives(&client, &family, limit, &ctx).await?;
    let source_raw_dir = raw_dir.join(&family.id);
    let source_parsed_dir = parsed_dir.join(&family.id);
    fs::create_dir_all(&source_raw_dir)?;
    fs::create_dir_all(&source_parsed_dir)?;

    let mut archives = Vec::new();
    let mut combined_tables = BTreeMap::<String, TableAccumulator>::new();
    let mut observed_schemas = Vec::new();

    for discovered_artifact in discovered {
        let local = fetch_archive(&client, &discovered_artifact, &source_raw_dir).await?;
        let parse = parse_local_archive(&local, &source_parsed_dir)?;
        observed_schemas.extend(parse.observed_schemas);
        for output in parse.raw_outputs {
            let entry = combined_tables
                .entry(output.logical_table_key.clone())
                .or_insert_with(|| {
                    TableAccumulator::new(&output.logical_table_key, &source_parsed_dir)
                });
            entry.push(
                &output.logical_table_key,
                &output.rows,
                output.schema_key.as_str(),
            );
        }

        archives.push(ArchiveManifest {
            artifact_id: local.metadata.artifact_id.clone(),
            archive_name: local
                .local_path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string(),
            archive_path: local.local_path.display().to_string(),
            source_url: local.metadata.acquisition_uri.clone(),
            sha256: local.metadata.content_sha256.clone().unwrap_or_default(),
            bytes: local.metadata.content_length_bytes.unwrap_or_default() as usize,
        });
    }

    let mut tables = Vec::new();
    for (_, accumulator) in combined_tables {
        tables.push(accumulator.finish()?);
    }
    tables.sort_by(|a, b| a.output_name.cmp(&b.output_name));

    Ok(NemwebIngestResult {
        source_family: family.id,
        source_url: family.listing_url,
        fetched_at_utc: Utc::now().to_rfc3339(),
        archives,
        tables,
        observed_schemas,
    })
}

#[derive(Debug, Clone)]
struct TableAccumulator {
    logical_table_key: String,
    output_name: String,
    output_path: PathBuf,
    batch_jsonl: String,
    rows_written: usize,
}

impl TableAccumulator {
    fn new(logical_table_key: &str, parsed_dir: &Path) -> Self {
        let output_name = sanitize_logical_table_key(logical_table_key);
        let output_path = parsed_dir.join(format!("{}.jsonl", output_name));
        Self {
            logical_table_key: logical_table_key.to_string(),
            output_name,
            output_path,
            batch_jsonl: String::new(),
            rows_written: 0,
        }
    }

    fn push(&mut self, logical_table_key: &str, rows: &[Value], schema_key: &str) {
        if rows.is_empty() {
            self.batch_jsonl.push_str(
                &json!({
                    "_logical_table_key": logical_table_key,
                    "_schema_key": schema_key,
                    "_note": "batch contained no rows",
                })
                .to_string(),
            );
            self.batch_jsonl.push('\n');
            self.rows_written += 1;
            return;
        }

        for payload in rows {
            self.batch_jsonl.push_str(&payload.to_string());
            self.batch_jsonl.push('\n');
        }
        self.rows_written += rows.len();
    }

    fn finish(self) -> Result<ParsedTableBatch> {
        if let Some(parent) = self.output_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&self.output_path, &self.batch_jsonl)?;
        Ok(ParsedTableBatch {
            logical_table_key: self.logical_table_key,
            output_name: self.output_name,
            output_path: self.output_path.display().to_string(),
            rows_written: self.rows_written,
            batch_jsonl: self.batch_jsonl,
            description: "Observed logical table batch from NEMweb archive(s).".to_string(),
        })
    }
}
