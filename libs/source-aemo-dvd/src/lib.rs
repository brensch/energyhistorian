use std::fs;
use std::io::{Cursor, Read};
use std::path::Path;

use anyhow::{Context, Result, bail};
use chrono::Utc;
use encoding_rs::WINDOWS_1252;
use ingest_core::{
    ArtifactKind, ArtifactMetadata, BoxedFuture, CollectionCompletion, CompletionUnit,
    DiscoveredArtifact, DiscoveryRequest, LocalArtifact, ParseResult, PluginCapabilities,
    PromotionSpec, RawPluginParseResult, RawPluginTableBatch, RawTableRowSink, RunContext,
    RuntimePluginParseResult, RuntimeSourcePlugin, SourceCollection, SourceDescriptor,
    SourceMetadataDocument, SourcePlugin, TaskBlueprint, TaskKind,
};
use regex::Regex;
use roxmltree::Document;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use zip::ZipArchive;

const MMSDM_ARCHIVE_ROOT: &str = "https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/";

#[derive(Clone)]
pub struct AemoMetadataDvdPlugin;

impl AemoMetadataDvdPlugin {
    pub fn new() -> Self {
        Self
    }

    pub async fn discover_collection(
        &self,
        client: &reqwest::Client,
        collection_id: &str,
        ctx: &RunContext,
    ) -> Result<Vec<DiscoveredArtifact>> {
        if collection_id != "monthly-metadata" {
            bail!("unknown DVD metadata collection '{collection_id}'");
        }
        discover_monthly_metadata(client, ctx).await
    }

    pub async fn fetch_artifact(
        &self,
        client: &reqwest::Client,
        artifact: &DiscoveredArtifact,
        output_dir: &Path,
    ) -> Result<LocalArtifact> {
        fs::create_dir_all(output_dir)?;
        let bytes = client
            .get(&artifact.metadata.acquisition_uri)
            .send()
            .await
            .with_context(|| format!("downloading {}", artifact.metadata.acquisition_uri))?
            .bytes()
            .await?
            .to_vec();
        let filename = artifact
            .metadata
            .acquisition_uri
            .rsplit('/')
            .next()
            .unwrap_or("artifact.zip");
        let local_path = output_dir.join(filename);
        fs::write(&local_path, &bytes)?;

        let mut metadata = artifact.metadata.clone();
        metadata.fetched_at = Some(Utc::now());
        metadata.content_sha256 = Some(format!("{:x}", Sha256::digest(&bytes)));
        metadata.content_length_bytes = Some(bytes.len() as u64);

        Ok(LocalArtifact {
            metadata,
            local_path,
        })
    }

    pub fn parse_artifact(&self, artifact: &LocalArtifact) -> Result<RawPluginParseResult> {
        parse_monthly_metadata(artifact)
    }
}

impl Default for AemoMetadataDvdPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl SourcePlugin for AemoMetadataDvdPlugin {
    fn descriptor(&self) -> SourceDescriptor {
        SourceDescriptor {
            source_id: "aemo_metadata_dvd".to_string(),
            domain: "metadata".to_string(),
            description: "AEMO monthly SQLLoader/DVD metadata archives.".to_string(),
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
                "Downloads versioned SQLLoader metadata assets directly from monthly MMSDM archives."
                    .to_string(),
                "Extracts raw table and column descriptions from DDL comments.".to_string(),
            ],
        }
    }

    fn collections(&self) -> Vec<SourceCollection> {
        vec![SourceCollection {
            id: "monthly-metadata".to_string(),
            display_name: "Monthly Metadata".to_string(),
            description: "Direct monthly SQLLoader metadata assets from MMSDM archive.".to_string(),
            retrieval_modes: vec!["discover-archive".to_string(), "fetch-zip".to_string()],
            completion: CollectionCompletion {
                unit: CompletionUnit::DocumentVersion,
                dedupe_keys: vec!["artifact_id".to_string(), "content_sha256".to_string()],
                cursor_field: Some("month_key".to_string()),
                mutable_window_seconds: Some(86_400),
                notes: vec![
                    "Fetches create and upgrade metadata zips directly, not whole monthly archives."
                        .to_string(),
                ],
            },
            task_blueprints: vec![
                TaskBlueprint {
                    kind: TaskKind::Discover,
                    description: "Discover monthly SQLLoader metadata assets.".to_string(),
                    max_concurrency: 1,
                    queue: "metadata-discover".to_string(),
                    idempotency_scope: "source+collection+artifact".to_string(),
                },
                TaskBlueprint {
                    kind: TaskKind::Fetch,
                    description: "Download monthly metadata zips directly.".to_string(),
                    max_concurrency: 2,
                    queue: "metadata-fetch".to_string(),
                    idempotency_scope: "artifact_id".to_string(),
                },
                TaskBlueprint {
                    kind: TaskKind::Parse,
                    description: "Extract release, table, and column metadata from DDL zips."
                        .to_string(),
                    max_concurrency: 2,
                    queue: "metadata-parse".to_string(),
                    idempotency_scope: "artifact_id+parser_version".to_string(),
                },
            ],
            default_poll_interval_seconds: Some(86_400),
        }]
    }

    fn metadata_catalog(&self) -> Vec<SourceMetadataDocument> {
        Vec::new()
    }

    fn discover(
        &self,
        _request: &DiscoveryRequest,
        _ctx: &RunContext,
    ) -> Result<Vec<DiscoveredArtifact>> {
        bail!("Use discover_collection() for async DVD metadata discovery")
    }

    fn fetch(&self, _artifact: &DiscoveredArtifact, _ctx: &RunContext) -> Result<LocalArtifact> {
        bail!("Use fetch_artifact() for async DVD metadata fetching")
    }

    fn inspect_parse(&self, _artifact: &LocalArtifact, _ctx: &RunContext) -> Result<ParseResult> {
        bail!("DVD metadata parsing targets plugin-owned raw tables, not schema-hash raw storage")
    }

    fn stream_parse(
        &self,
        _artifact: &LocalArtifact,
        _ctx: &RunContext,
        _sink: &mut dyn RawTableRowSink,
    ) -> Result<()> {
        bail!("DVD metadata parsing targets plugin-owned raw tables, not schema-hash raw storage")
    }

    fn promotion_plan(&self) -> &'static [PromotionSpec] {
        &[]
    }
}

impl RuntimeSourcePlugin for AemoMetadataDvdPlugin {
    fn parser_version(&self) -> &'static str {
        "source-aemo-dvd/0.1"
    }

    fn discover_collection_async<'a>(
        &'a self,
        client: &'a reqwest::Client,
        collection_id: &'a str,
        _limit: usize,
        ctx: &'a RunContext,
    ) -> BoxedFuture<'a, Result<Vec<DiscoveredArtifact>>> {
        Box::pin(async move { self.discover_collection(client, collection_id, ctx).await })
    }

    fn fetch_artifact_async<'a>(
        &'a self,
        client: &'a reqwest::Client,
        _collection_id: &'a str,
        artifact: &'a DiscoveredArtifact,
        output_dir: &'a Path,
    ) -> BoxedFuture<'a, Result<LocalArtifact>> {
        Box::pin(async move { self.fetch_artifact(client, artifact, output_dir).await })
    }

    fn parse_artifact_runtime(
        &self,
        _collection_id: &str,
        artifact: LocalArtifact,
        _ctx: &RunContext,
    ) -> Result<RuntimePluginParseResult> {
        let result = self.parse_artifact(&artifact)?;
        Ok(RuntimePluginParseResult::RawMetadata { artifact, result })
    }

    fn stream_structured_parse_runtime(
        &self,
        _artifact: &LocalArtifact,
        _collection_id: &str,
        _ctx: &RunContext,
        _sink: &mut dyn RawTableRowSink,
    ) -> Result<()> {
        bail!("metadata DVD source does not stream structured raw rows")
    }
}

async fn discover_monthly_metadata(
    client: &reqwest::Client,
    ctx: &RunContext,
) -> Result<Vec<DiscoveredArtifact>> {
    let years_html = fetch_text(client, MMSDM_ARCHIVE_ROOT).await?;
    let year_re = Regex::new(r#"/MMSDM/(\d{4})/"#)?;
    let month_re = Regex::new(r#"(MMSDM_(\d{4})_(\d{2}))/"#)?;
    let href_re = Regex::new(r#"HREF="([^"]+)""#)?;

    let mut years = year_re
        .captures_iter(&years_html)
        .filter_map(|caps| caps.get(1).map(|m| m.as_str().to_string()))
        .collect::<Vec<_>>();
    years.sort();
    years.dedup();

    let mut artifacts = Vec::new();
    for year in years {
        let year_url = format!("{MMSDM_ARCHIVE_ROOT}{year}/");
        let year_html = fetch_text(client, &year_url).await?;
        let mut months = month_re
            .captures_iter(&year_html)
            .filter_map(|caps| {
                Some((
                    caps.get(1)?.as_str().to_string(),
                    format!("{}-{}", caps.get(2)?.as_str(), caps.get(3)?.as_str()),
                ))
            })
            .collect::<Vec<_>>();
        months.sort();
        months.dedup();

        for (month_dir, month_key) in months {
            let docs_url = format!(
                "{year_url}{month_dir}/MMSDM_Historical_Data_SQLLoader/DOCUMENTATION/MMS%20Data%20Model/"
            );
            let docs_html = fetch_text(client, &docs_url).await.unwrap_or_default();
            let mut version_dirs = href_re
                .captures_iter(&docs_html)
                .filter_map(|caps| caps.get(1).map(|m| m.as_str().to_string()))
                .filter_map(|href| {
                    let trimmed = href.trim_end_matches('/');
                    let version = trimmed.rsplit('/').next()?;
                    version
                        .strip_prefix('v')
                        .filter(|value| !value.is_empty())
                        .map(str::to_string)
                })
                .collect::<Vec<_>>();
            version_dirs.sort();
            version_dirs.dedup();

            for version in version_dirs {
                let version_url = format!("{docs_url}v{version}/");
                let version_html = fetch_text(client, &version_url).await.unwrap_or_default();
                for href in href_re
                    .captures_iter(&version_html)
                    .filter_map(|caps| caps.get(1).map(|m| m.as_str().to_string()))
                {
                    let artifact_type = if href.contains("MMSDM_create_v") && href.ends_with(".zip")
                    {
                        Some("create_zip")
                    } else if href.contains("MMSDM_upgrade_v") && href.ends_with(".zip") {
                        Some("upgrade_zip")
                    } else {
                        None
                    };
                    let Some(artifact_type) = artifact_type else {
                        continue;
                    };
                    let url = if href.starts_with("http") {
                        href.clone()
                    } else if href.starts_with('/') {
                        format!("https://nemweb.com.au{href}")
                    } else {
                        format!("{version_url}{href}")
                    };
                    artifacts.push(DiscoveredArtifact {
                        metadata: ArtifactMetadata {
                            artifact_id: format!(
                                "aemo_metadata_dvd:monthly-metadata:{month_key}:v{}:{artifact_type}",
                                sanitize_part(&version)
                            ),
                            source_id: "aemo_metadata_dvd".to_string(),
                            acquisition_uri: url,
                            discovered_at: Utc::now(),
                            fetched_at: None,
                            published_at: None,
                            content_sha256: None,
                            content_length_bytes: None,
                            kind: ArtifactKind::ZipArchive,
                            parser_version: ctx.parser_version.clone(),
                            model_version: Some(version.clone()),
                            release_name: Some(month_key.clone()),
                        },
                    });
                }
            }
        }
    }

    Ok(artifacts)
}

fn parse_monthly_metadata(artifact: &LocalArtifact) -> Result<RawPluginParseResult> {
    let bytes = fs::read(&artifact.local_path)?;
    let mut zip = ZipArchive::new(Cursor::new(bytes)).context("opening metadata zip")?;
    let zip_name = artifact
        .local_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default()
        .to_string();
    let artifact_type = if zip_name.contains("create") {
        "create_zip"
    } else if zip_name.contains("upgrade") {
        "upgrade_zip"
    } else {
        "zip"
    };
    let month_key = extract_month_key(&artifact.metadata.acquisition_uri);
    let model_version = artifact.metadata.model_version.clone();

    let mut tables = vec![RawPluginTableBatch {
        table_name: "monthly_artifacts".to_string(),
        rows: vec![json!({
            "month_key": month_key,
            "model_version": model_version,
            "artifact_type": artifact_type,
            "artifact_name": zip_name,
            "artifact_url": artifact.metadata.acquisition_uri,
            "local_path": artifact.local_path.display().to_string(),
            "content_sha256": artifact.metadata.content_sha256,
            "content_length_bytes": artifact.metadata.content_length_bytes,
        })],
    }];

    if let Some(schema_xml) = zip_entry_string(&mut zip, "schema.xml")? {
        let (releases, artefacts) = parse_schema_xml(&schema_xml)?;
        if !releases.is_empty() {
            tables.push(RawPluginTableBatch {
                table_name: "releases".to_string(),
                rows: releases,
            });
        }
        if !artefacts.is_empty() {
            tables.push(RawPluginTableBatch {
                table_name: "release_artifacts".to_string(),
                rows: artefacts,
            });
        }
    }

    if artifact_type == "create_zip" {
        if let Some(sql) = best_sql_script(&mut zip, "create_mms_data_model.sql")? {
            let table_columns = parse_table_columns(&sql);
            let tables_rows =
                parse_table_comments(&sql, month_key.as_deref(), model_version.as_deref());
            let column_rows =
                parse_column_comments(&sql, month_key.as_deref(), model_version.as_deref());
            let pk_rows = parse_primary_keys(&sql, month_key.as_deref(), model_version.as_deref());

            if !tables_rows.is_empty() {
                tables.push(RawPluginTableBatch {
                    table_name: "tables".to_string(),
                    rows: tables_rows,
                });
            }
            if !column_rows.is_empty() {
                tables.push(RawPluginTableBatch {
                    table_name: "columns".to_string(),
                    rows: column_rows,
                });
            }
            if !table_columns.is_empty() {
                tables.push(RawPluginTableBatch {
                    table_name: "table_columns".to_string(),
                    rows: table_columns,
                });
            }
            if !pk_rows.is_empty() {
                tables.push(RawPluginTableBatch {
                    table_name: "primary_keys".to_string(),
                    rows: pk_rows,
                });
            }
        }
    }

    if artifact_type == "upgrade_zip" {
        if let Some(sql) = best_sql_script(&mut zip, "alter_mms_data_model.sql")? {
            let rows = parse_upgrade_changes(&sql, month_key.as_deref(), model_version.as_deref());
            if !rows.is_empty() {
                tables.push(RawPluginTableBatch {
                    table_name: "upgrade_changes".to_string(),
                    rows,
                });
            }
        }
    }

    Ok(RawPluginParseResult { tables })
}

async fn fetch_text(client: &reqwest::Client, url: &str) -> Result<String> {
    client
        .get(url)
        .send()
        .await
        .with_context(|| format!("downloading {url}"))?
        .text()
        .await
        .with_context(|| format!("reading {url}"))
}

fn zip_entry_string(zip: &mut ZipArchive<Cursor<Vec<u8>>>, suffix: &str) -> Result<Option<String>> {
    for idx in 0..zip.len() {
        let mut file = zip.by_index(idx)?;
        if file.name().ends_with(suffix) {
            let mut out = Vec::new();
            file.read_to_end(&mut out)?;
            return Ok(Some(decode_zip_text(&out)));
        }
    }
    Ok(None)
}

fn best_sql_script(
    zip: &mut ZipArchive<Cursor<Vec<u8>>>,
    script_name: &str,
) -> Result<Option<String>> {
    for prefix in ["postgreSQL", "Oracle", "mySQL", "SQLServer", "Snowflake"] {
        for idx in 0..zip.len() {
            let mut file = zip.by_index(idx)?;
            if file.name().contains(prefix) && file.name().ends_with(script_name) {
                let mut out = Vec::new();
                file.read_to_end(&mut out)?;
                return Ok(Some(decode_zip_text(&out)));
            }
        }
    }
    Ok(None)
}

fn decode_zip_text(bytes: &[u8]) -> String {
    if let Ok(text) = std::str::from_utf8(bytes) {
        return text.to_string();
    }

    if bytes.starts_with(&[0xFF, 0xFE]) {
        return decode_utf16(&bytes[2..], true);
    }
    if bytes.starts_with(&[0xFE, 0xFF]) {
        return decode_utf16(&bytes[2..], false);
    }

    let nul_count = bytes.iter().take(1024).filter(|&&byte| byte == 0).count();
    if nul_count > 64 {
        let le_even_nuls = bytes
            .iter()
            .step_by(2)
            .take(512)
            .filter(|&&byte| byte == 0)
            .count();
        let le_odd_nuls = bytes
            .iter()
            .skip(1)
            .step_by(2)
            .take(512)
            .filter(|&&byte| byte == 0)
            .count();
        return if le_odd_nuls > le_even_nuls {
            decode_utf16(bytes, true)
        } else {
            decode_utf16(bytes, false)
        };
    }

    WINDOWS_1252.decode(bytes).0.into_owned()
}

fn decode_utf16(bytes: &[u8], little_endian: bool) -> String {
    let code_units = bytes
        .chunks_exact(2)
        .map(|chunk| {
            if little_endian {
                u16::from_le_bytes([chunk[0], chunk[1]])
            } else {
                u16::from_be_bytes([chunk[0], chunk[1]])
            }
        })
        .collect::<Vec<_>>();
    String::from_utf16_lossy(&code_units)
}

fn parse_schema_xml(schema_xml: &str) -> Result<(Vec<Value>, Vec<Value>)> {
    let doc = Document::parse(schema_xml)?;
    let mut releases = Vec::new();
    let mut artefacts = Vec::new();

    for change in doc.descendants().filter(|node| node.has_tag_name("Change")) {
        let version = attr(change, "Id");
        releases.push(json!({
            "model_version": version,
            "script_version": attr(change, "ScriptVersion"),
            "release_date": attr(change, "ReleaseDate"),
            "change_notice": attr(change, "ChangeNotice"),
            "description": child_text(change, "Description"),
        }));

        for artefact in change
            .children()
            .filter(|node| node.has_tag_name("Artefact"))
        {
            for script in artefact
                .descendants()
                .filter(|node| node.has_tag_name("Script"))
            {
                artefacts.push(json!({
                    "model_version": version,
                    "artefact_order": attr(artefact, "Order"),
                    "artefact_type": attr(artefact, "Type"),
                    "artefact_description": attr(artefact, "Description"),
                    "database_name": attr(script, "Database"),
                    "script_name": attr(script, "Name"),
                }));
            }
        }
    }

    Ok((releases, artefacts))
}

fn parse_table_comments(
    sql: &str,
    month_key: Option<&str>,
    model_version: Option<&str>,
) -> Vec<Value> {
    let re = Regex::new(r#"(?is)comment\s+on\s+table\s+([A-Z0-9_]+)\s+is\s+'(.*?)';"#).unwrap();
    re.captures_iter(sql)
        .map(|caps| {
            json!({
                "month_key": month_key,
                "model_version": model_version,
                "table_name": caps[1].to_string(),
                "description": clean_sql_text(&caps[2]),
            })
        })
        .collect()
}

fn parse_column_comments(
    sql: &str,
    month_key: Option<&str>,
    model_version: Option<&str>,
) -> Vec<Value> {
    let re =
        Regex::new(r#"(?is)comment\s+on\s+column\s+([A-Z0-9_]+)\.([A-Z0-9_]+)\s+is\s+'(.*?)';"#)
            .unwrap();
    re.captures_iter(sql)
        .map(|caps| {
            json!({
                "month_key": month_key,
                "model_version": model_version,
                "table_name": caps[1].to_string(),
                "column_name": caps[2].to_string(),
                "description": clean_sql_text(&caps[3]),
            })
        })
        .collect()
}

fn parse_table_columns(sql: &str) -> Vec<Value> {
    let create_re = Regex::new(r#"(?is)create\s+table\s+([A-Z0-9_]+)\s*\((.*?)\);\s"#).unwrap();
    let mut rows = Vec::new();

    for caps in create_re.captures_iter(sql) {
        let table_name = caps[1].to_string();
        let body = caps[2].to_string();
        for (ordinal, line) in body.lines().enumerate() {
            let trimmed = line.trim().trim_end_matches(',');
            if trimmed.is_empty()
                || trimmed.starts_with("constraint ")
                || trimmed.starts_with("primary key")
            {
                continue;
            }
            let mut parts = trimmed.split_whitespace();
            let Some(column_name) = parts.next() else {
                continue;
            };
            let data_type = parts.next().unwrap_or_default().to_string();
            if column_name.eq_ignore_ascii_case("constraint") {
                continue;
            }
            rows.push(json!({
                "table_name": table_name,
                "column_name": column_name,
                "ordinal": ordinal,
                "data_type": data_type,
                "nullable": !trimmed.to_ascii_lowercase().contains("not null"),
            }));
        }
    }

    rows
}

fn parse_primary_keys(
    sql: &str,
    month_key: Option<&str>,
    model_version: Option<&str>,
) -> Vec<Value> {
    let re = Regex::new(
        r#"(?is)alter\s+table\s+([A-Z0-9_]+)\s+add\s+constraint\s+([A-Z0-9_]+)\s+primary\s+key\s*\((.*?)\)"#,
    )
    .unwrap();
    let mut rows = Vec::new();
    for caps in re.captures_iter(sql) {
        let table = caps[1].to_string();
        let constraint = caps[2].to_string();
        for column in caps[3]
            .split(',')
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
        {
            rows.push(json!({
                "month_key": month_key,
                "model_version": model_version,
                "table_name": table,
                "constraint_name": constraint,
                "column_name": column,
            }));
        }
    }
    rows
}

fn parse_upgrade_changes(
    sql: &str,
    month_key: Option<&str>,
    model_version: Option<&str>,
) -> Vec<Value> {
    sql.split(';')
        .map(str::trim)
        .filter(|statement| !statement.is_empty())
        .filter_map(|statement| {
            let lower = statement.to_ascii_lowercase();
            let action_type = if lower.starts_with("create table") {
                Some("create_table")
            } else if lower.starts_with("alter table") && lower.contains(" add ") {
                Some("alter_add")
            } else if lower.starts_with("alter table") && lower.contains(" drop ") {
                Some("alter_drop")
            } else if lower.starts_with("comment on table") {
                Some("comment_table")
            } else if lower.starts_with("comment on column") {
                Some("comment_column")
            } else {
                None
            }?;

            let table_name = extract_table_name(statement);
            let column_name = extract_column_name(statement);
            Some(json!({
                "month_key": month_key,
                "model_version": model_version,
                "action_type": action_type,
                "table_name": table_name,
                "column_name": column_name,
                "statement": statement,
            }))
        })
        .collect()
}

fn extract_month_key(uri: &str) -> Option<String> {
    let re = Regex::new(r#"MMSDM_(\d{4})_(\d{2})"#).unwrap();
    re.captures(uri)
        .map(|caps| format!("{}-{}", &caps[1], &caps[2]))
}

fn extract_table_name(statement: &str) -> Option<String> {
    for re in [
        Regex::new(r#"(?i)(?:create|alter)\s+table\s+([A-Z0-9_]+)"#).unwrap(),
        Regex::new(r#"(?i)comment\s+on\s+table\s+([A-Z0-9_]+)"#).unwrap(),
        Regex::new(r#"(?i)comment\s+on\s+column\s+([A-Z0-9_]+)\.[A-Z0-9_]+"#).unwrap(),
    ] {
        if let Some(caps) = re.captures(statement) {
            return Some(caps[1].to_string());
        }
    }
    None
}

fn extract_column_name(statement: &str) -> Option<String> {
    let re = Regex::new(r#"(?i)comment\s+on\s+column\s+[A-Z0-9_]+\.([A-Z0-9_]+)"#).unwrap();
    re.captures(statement).map(|caps| caps[1].to_string())
}

fn clean_sql_text(input: &str) -> String {
    input
        .replace("''", "'")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn child_text(node: roxmltree::Node<'_, '_>, tag_name: &str) -> Option<String> {
    node.children()
        .find(|child| child.has_tag_name(tag_name))
        .and_then(|child| child.text())
        .map(str::to_string)
}

fn attr(node: roxmltree::Node<'_, '_>, name: &str) -> Option<String> {
    node.attribute(name).map(str::to_string)
}

fn sanitize_part(value: &str) -> String {
    value
        .to_ascii_lowercase()
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{decode_utf16, decode_zip_text};

    #[test]
    fn decodes_utf8_bytes() {
        let text = decode_zip_text(b"comment on table TEST is 'Plain ASCII';");
        assert!(text.contains("comment on table TEST"));
    }

    #[test]
    fn decodes_non_utf8_bytes_lossily() {
        let text = decode_zip_text(&[0x63, 0x6f, 0x6d, 0x6d, 0x65, 0x6e, 0x74, 0x20, 0x96]);
        assert_eq!(text, "comment \u{2013}");
    }

    #[test]
    fn decodes_utf16le_bytes() {
        let bytes = [
            0x63, 0x00, 0x6f, 0x00, 0x6d, 0x00, 0x6d, 0x00, 0x65, 0x00, 0x6e, 0x00, 0x74, 0x00,
        ];
        assert_eq!(decode_utf16(&bytes, true), "comment");
    }
}
