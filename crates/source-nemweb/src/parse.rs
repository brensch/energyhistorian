use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;

use anyhow::{Context, Result, anyhow, bail};
use chrono::Utc;
use csv::StringRecord;
use ingest_core::{
    ColumnTypeInference, LocalArtifact, LogicalTableId, ObservedSchema, ParseResult,
    PromotionMapping, RawTableChunk, SchemaApprovalStatus, SchemaColumn, SchemaVersionKey,
};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::cmp::Ordering;
use std::path::Path;
use zip::ZipArchive;

pub fn parse_local_archive(artifact: &LocalArtifact, _parsed_dir: &Path) -> Result<ParseResult> {
    let file = File::open(&artifact.local_path)
        .with_context(|| format!("opening {}", artifact.local_path.display()))?;
    let mut zip = ZipArchive::new(BufReader::new(file))
        .with_context(|| format!("opening ZIP archive {}", artifact.local_path.display()))?;
    if zip.is_empty() {
        bail!("archive {} is empty", artifact.local_path.display());
    }

    let entry = zip.by_index(0)?;
    let csv_name = entry.name().to_string();

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_reader(BufReader::new(entry));

    let mut headers_by_key = HashMap::<(String, String, String), Vec<String>>::new();
    let mut key_order = Vec::<(String, String, String)>::new();
    let mut inference_by_key = HashMap::<(String, String, String), ColumnTypeInference>::new();
    let mut pending_outputs = Vec::<PendingRawOutput>::new();

    for row in reader.records() {
        let record = row?;
        if record.is_empty() {
            continue;
        }

        match record.get(0) {
            Some("I") => {
                if let Some(key) = record_key(&record) {
                    let headers = record
                        .iter()
                        .skip(4)
                        .map(|value| value.to_string())
                        .collect::<Vec<_>>();
                    if !headers_by_key.contains_key(&key) {
                        key_order.push(key.clone());
                    }
                    inference_by_key
                        .entry(key.clone())
                        .or_insert_with(|| ColumnTypeInference::new(headers.len()));
                    headers_by_key.insert(key, headers);
                }
            }
            Some("D") => {
                let Some(key) = record_key(&record) else {
                    continue;
                };
                let headers = headers_by_key
                    .get(&key)
                    .ok_or_else(|| anyhow!("data row before header for {:?}", key))?;
                inference_by_key
                    .entry(key.clone())
                    .or_insert_with(|| ColumnTypeInference::new(headers.len()))
                    .update(record.iter().skip(4));
                let payload = record_to_json(
                    &record,
                    headers,
                    artifact.metadata.source_id.as_str(),
                    &csv_name,
                    &artifact.metadata.acquisition_uri,
                );
                pending_outputs.push(PendingRawOutput { key, payload });
            }
            _ => {}
        }
    }

    let mut observed_schemas = Vec::<ObservedSchema>::new();
    let mut schema_hash_by_key = HashMap::<(String, String, String), String>::new();
    for key in key_order {
        let headers = headers_by_key
            .get(&key)
            .ok_or_else(|| anyhow!("missing headers for {:?}", key))?;
        let inferred_types = inference_by_key
            .get(&key)
            .map(ColumnTypeInference::inferred_clickhouse_types)
            .unwrap_or_else(|| vec!["Nullable(Float64)".to_string(); headers.len()]);
        let schema = observed_schema_from_header(artifact, &key, headers, &inferred_types)?;
        schema_hash_by_key.insert(key, schema.schema_key.header_hash.clone());
        observed_schemas.push(schema);
    }

    let mut rows_by_output = HashMap::<((String, String, String), String), Vec<Value>>::new();
    for pending in pending_outputs {
        let schema_key = schema_hash_by_key
            .get(&pending.key)
            .ok_or_else(|| anyhow!("missing schema hash for {:?}", pending.key))?;
        rows_by_output
            .entry((pending.key, schema_key.clone()))
            .or_default()
            .push(pending.payload);
    }

    let mut raw_outputs = Vec::<RawTableChunk>::new();
    for ((key, schema_key), rows) in rows_by_output {
        raw_outputs.push(RawTableChunk {
            logical_table_key: logical_table_key(
                &artifact.metadata.source_id,
                &key.0,
                &key.1,
                &key.2,
            ),
            schema_key,
            rows,
        });
    }

    Ok(ParseResult {
        observed_schemas,
        raw_outputs,
        promotions: Vec::<PromotionMapping>::new(),
    })
}

fn observed_schema_from_header(
    artifact: &LocalArtifact,
    key: &(String, String, String),
    headers: &[String],
    inferred_types: &[String],
) -> Result<ObservedSchema> {
    let columns = headers
        .iter()
        .enumerate()
        .map(|(idx, header)| SchemaColumn {
            ordinal: idx + 1,
            name: header.clone(),
            source_data_type: inferred_types.get(idx).cloned(),
            nullable: None,
            primary_key: None,
            description: None,
        })
        .collect::<Vec<_>>();
    let hash = typed_schema_hash(key, &columns);
    Ok(ObservedSchema {
        schema_id: format!(
            "{}:{}:{}:{}:{}",
            artifact.metadata.source_id, key.0, key.1, key.2, hash
        ),
        schema_key: SchemaVersionKey {
            logical_table: LogicalTableId {
                source_family: artifact.metadata.source_id.to_uppercase(),
                section: key.0.clone(),
                table: key.1.clone(),
            },
            report_version: key.2.clone(),
            model_version: artifact.metadata.model_version.clone(),
            header_hash: hash,
        },
        first_seen_at: Utc::now(),
        last_seen_at: Utc::now(),
        observed_in_artifact_id: artifact.metadata.artifact_id.clone(),
        columns,
        approval_status: SchemaApprovalStatus::Proposed,
    })
}

fn record_key(record: &StringRecord) -> Option<(String, String, String)> {
    Some((
        record.get(1)?.to_string(),
        record.get(2)?.to_string(),
        record.get(3)?.to_string(),
    ))
}

pub fn logical_table_key(source_family: &str, section: &str, table: &str, version: &str) -> String {
    format!(
        "{}/{}/{}/{}",
        source_family.to_uppercase(),
        section,
        table,
        version
    )
}

pub fn sanitize_logical_table_key(logical_table_key: &str) -> String {
    logical_table_key
        .to_ascii_lowercase()
        .replace(['/', '-'], "_")
}

fn typed_schema_hash(key: &(String, String, String), columns: &[SchemaColumn]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(key.0.as_bytes());
    hasher.update(b"|");
    hasher.update(key.1.as_bytes());
    hasher.update(b"|");
    hasher.update(key.2.as_bytes());
    hasher.update(b"|");
    let mut sorted_columns = columns.iter().collect::<Vec<_>>();
    sorted_columns.sort_by(|left, right| match left.name.cmp(&right.name) {
        Ordering::Equal => left
            .source_data_type
            .as_deref()
            .unwrap_or("String")
            .cmp(right.source_data_type.as_deref().unwrap_or("String")),
        other => other,
    });
    for column in sorted_columns {
        hasher.update(column.name.as_bytes());
        hasher.update(b":");
        hasher.update(
            column
                .source_data_type
                .as_deref()
                .unwrap_or("String")
                .as_bytes(),
        );
        hasher.update(b",");
    }
    format!("{:x}", hasher.finalize())
}

struct PendingRawOutput {
    key: (String, String, String),
    payload: Value,
}

fn record_to_json(
    record: &StringRecord,
    headers: &[String],
    source_id: &str,
    csv_name: &str,
    source_url: &str,
) -> Value {
    let mut payload = Map::new();
    payload.insert("_source".into(), Value::String(source_id.to_string()));
    payload.insert("_archive_entry".into(), Value::String(csv_name.to_string()));
    payload.insert("_source_url".into(), Value::String(source_url.to_string()));
    payload.insert(
        "_ingested_at_utc".into(),
        Value::String(Utc::now().to_rfc3339()),
    );
    payload.insert("_section".into(), json!(record.get(1).unwrap_or_default()));
    payload.insert("_table".into(), json!(record.get(2).unwrap_or_default()));
    payload.insert("_version".into(), json!(record.get(3).unwrap_or_default()));

    for (header, value) in headers.iter().zip(record.iter().skip(4)) {
        payload.insert(header.to_lowercase(), parse_scalar(value));
    }

    Value::Object(payload)
}

fn parse_scalar(value: &str) -> Value {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Value::Null;
    }
    if let Ok(integer) = trimmed.parse::<i64>() {
        return json!(integer);
    }
    if let Ok(float) = trimmed.parse::<f64>() {
        return json!(float);
    }
    Value::String(trimmed.to_string())
}
