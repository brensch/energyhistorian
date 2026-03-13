use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use anyhow::{Context, Result, anyhow, bail};
use chrono::Utc;
use csv::StringRecord;
use ingest_core::{
    ColumnTypeInference, LocalArtifact, LogicalTableId, ObservedSchema, ParseResult,
    PromotionMapping, RawTableChunk, RawTableRow, RawTableRowSink, RawValue, SchemaApprovalStatus,
    SchemaColumn, SchemaVersionKey, StructuredRawEvent, StructuredRawEventSink, StructuredRow,
    nem_time::{parse_nem_date, parse_nem_datetime},
};
use sha2::{Digest, Sha256};
use zip::ZipArchive;

type RecordKey = (String, String, String);

#[derive(Debug, Clone)]
pub struct ArchiveParsePlan {
    pub observed_schemas: Vec<ObservedSchema>,
    pub raw_outputs: Vec<RawTableChunk>,
    headers_by_key: HashMap<RecordKey, Vec<String>>,
    column_types_by_key: HashMap<RecordKey, Vec<String>>,
    schema_hash_by_key: HashMap<RecordKey, String>,
    csv_name: String,
}

struct TableState {
    headers: Vec<String>,
    inference: ColumnTypeInference,
    buffered_records: Vec<StringRecord>,
    inferred_types: Option<Vec<String>>,
    schema_key: Option<String>,
    row_count: usize,
}

impl TableState {
    fn new(headers: Vec<String>) -> Self {
        Self {
            inference: ColumnTypeInference::new(headers.len()),
            headers,
            buffered_records: Vec::new(),
            inferred_types: None,
            schema_key: None,
            row_count: 0,
        }
    }
}

pub fn inspect_local_archive(artifact: &LocalArtifact) -> Result<ArchiveParsePlan> {
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

    let mut headers_by_key = HashMap::<RecordKey, Vec<String>>::new();
    let mut key_order = Vec::<RecordKey>::new();
    let mut inference_by_key = HashMap::<RecordKey, ColumnTypeInference>::new();
    let mut row_count_by_key = HashMap::<RecordKey, usize>::new();

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
                *row_count_by_key.entry(key).or_default() += 1;
            }
            _ => {}
        }
    }

    let mut observed_schemas = Vec::<ObservedSchema>::new();
    let mut column_types_by_key = HashMap::<RecordKey, Vec<String>>::new();
    let mut schema_hash_by_key = HashMap::<RecordKey, String>::new();
    let mut raw_outputs = Vec::<RawTableChunk>::new();
    for key in &key_order {
        let headers = headers_by_key
            .get(key)
            .ok_or_else(|| anyhow!("missing headers for {:?}", key))?;
        let inferred_types = inference_by_key
            .get(key)
            .map(ColumnTypeInference::inferred_clickhouse_types)
            .unwrap_or_else(|| vec!["Nullable(Float64)".to_string(); headers.len()]);
        let schema = observed_schema_from_header(artifact, key, headers, &inferred_types)?;
        let schema_key = schema.schema_key.header_hash.clone();
        let row_count = row_count_by_key.get(key).copied().unwrap_or_default();
        column_types_by_key.insert(key.clone(), inferred_types);
        schema_hash_by_key.insert(key.clone(), schema_key.clone());
        observed_schemas.push(schema);
        if row_count > 0 {
            raw_outputs.push(RawTableChunk {
                logical_table_key: logical_table_key(
                    &artifact.metadata.source_id,
                    &key.0,
                    &key.1,
                    &key.2,
                ),
                schema_key,
                row_count,
                rows: Vec::new(),
            });
        }
    }

    Ok(ArchiveParsePlan {
        observed_schemas,
        raw_outputs,
        headers_by_key,
        column_types_by_key,
        schema_hash_by_key,
        csv_name,
    })
}

pub fn parse_local_archive(artifact: &LocalArtifact, _parsed_dir: &Path) -> Result<ParseResult> {
    let mut sink = MaterializingEventSink::default();
    stream_local_archive_events(artifact, &mut sink)?;
    Ok(sink.into_parse_result())
}

pub fn stream_local_archive_rows(
    artifact: &LocalArtifact,
    plan: &ArchiveParsePlan,
    sink: &mut dyn RawTableRowSink,
) -> Result<()> {
    let file = File::open(&artifact.local_path)
        .with_context(|| format!("opening {}", artifact.local_path.display()))?;
    let mut zip = ZipArchive::new(BufReader::new(file))
        .with_context(|| format!("opening ZIP archive {}", artifact.local_path.display()))?;
    if zip.is_empty() {
        bail!("archive {} is empty", artifact.local_path.display());
    }

    let entry = zip.by_index(0)?;
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_reader(BufReader::new(entry));

    for row in reader.records() {
        let record = row?;
        if record.is_empty() || record.get(0) != Some("D") {
            continue;
        }

        let Some(key) = record_key(&record) else {
            continue;
        };
        let headers = plan
            .headers_by_key
            .get(&key)
            .ok_or_else(|| anyhow!("data row before header for {:?}", key))?;
        let column_types = plan
            .column_types_by_key
            .get(&key)
            .ok_or_else(|| anyhow!("missing inferred types for {:?}", key))?;
        let schema_key = plan
            .schema_hash_by_key
            .get(&key)
            .cloned()
            .ok_or_else(|| anyhow!("missing schema hash for {:?}", key))?;
        sink.accept(RawTableRow {
            logical_table_key: logical_table_key(
                &artifact.metadata.source_id,
                &key.0,
                &key.1,
                &key.2,
            ),
            schema_key,
            row: record_to_structured_row(
                &record,
                headers,
                column_types,
                &plan.csv_name,
                &artifact.metadata.acquisition_uri,
            )?,
        })?;
    }

    Ok(())
}

pub fn stream_local_archive_events(
    artifact: &LocalArtifact,
    sink: &mut dyn StructuredRawEventSink,
) -> Result<()> {
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

    let mut key_order = Vec::<RecordKey>::new();
    let mut states = HashMap::<RecordKey, TableState>::new();
    for row in reader.records() {
        let record = row?;
        if record.is_empty() {
            continue;
        }

        match record.get(0) {
            Some("I") => {
                let Some(key) = record_key(&record) else {
                    continue;
                };
                let headers = record
                    .iter()
                    .skip(4)
                    .map(|value| value.to_string())
                    .collect::<Vec<_>>();
                if !states.contains_key(&key) {
                    key_order.push(key.clone());
                }
                states.insert(key, TableState::new(headers));
            }
            Some("D") => {
                let Some(key) = record_key(&record) else {
                    continue;
                };
                let state = states
                    .get_mut(&key)
                    .ok_or_else(|| anyhow!("data row before header for {:?}", key))?;
                state.row_count += 1;

                if let (Some(column_types), Some(schema_key)) =
                    (state.inferred_types.as_deref(), state.schema_key.as_deref())
                {
                    sink.accept(StructuredRawEvent::Row(RawTableRow {
                        logical_table_key: logical_table_key(
                            &artifact.metadata.source_id,
                            &key.0,
                            &key.1,
                            &key.2,
                        ),
                        schema_key: schema_key.to_string(),
                        row: record_to_structured_row(
                            &record,
                            &state.headers,
                            column_types,
                            &csv_name,
                            &artifact.metadata.acquisition_uri,
                        )?,
                    }))?;
                    continue;
                }

                state.inference.update(record.iter().skip(4));
                state.buffered_records.push(record.clone());
                if state.inference.reached_scan_limit() {
                    finalize_table_state(artifact, &key, &csv_name, state, sink)?;
                }
            }
            _ => {}
        }
    }

    for key in key_order {
        let Some(state) = states.get_mut(&key) else {
            continue;
        };
        finalize_table_state(artifact, &key, &csv_name, state, sink)?;
    }

    Ok(())
}

fn finalize_table_state(
    artifact: &LocalArtifact,
    key: &RecordKey,
    csv_name: &str,
    state: &mut TableState,
    sink: &mut dyn StructuredRawEventSink,
) -> Result<()> {
    if state.inferred_types.is_none() {
        let inferred_types = state.inference.inferred_clickhouse_types();
        let schema = observed_schema_from_header(artifact, key, &state.headers, &inferred_types)?;
        state.schema_key = Some(schema.schema_key.header_hash.clone());
        state.inferred_types = Some(inferred_types);
        sink.accept(StructuredRawEvent::Schema(schema))?;
    }

    let column_types = state
        .inferred_types
        .as_deref()
        .ok_or_else(|| anyhow!("missing inferred types for {:?}", key))?;
    let schema_key = state
        .schema_key
        .as_deref()
        .ok_or_else(|| anyhow!("missing schema key for {:?}", key))?;
    if state.buffered_records.is_empty() {
        return Ok(());
    }

    let logical_table_key = logical_table_key(&artifact.metadata.source_id, &key.0, &key.1, &key.2);
    for record in state.buffered_records.drain(..) {
        sink.accept(StructuredRawEvent::Row(RawTableRow {
            logical_table_key: logical_table_key.clone(),
            schema_key: schema_key.to_string(),
            row: record_to_structured_row(
                &record,
                &state.headers,
                column_types,
                csv_name,
                &artifact.metadata.acquisition_uri,
            )?,
        }))?;
    }

    Ok(())
}

fn observed_schema_from_header(
    artifact: &LocalArtifact,
    key: &RecordKey,
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

fn record_key(record: &StringRecord) -> Option<RecordKey> {
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

fn typed_schema_hash(key: &RecordKey, columns: &[SchemaColumn]) -> String {
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

fn record_to_structured_row(
    record: &StringRecord,
    headers: &[String],
    column_types: &[String],
    csv_name: &str,
    source_url: &str,
) -> Result<StructuredRow> {
    let mut values = Vec::with_capacity(headers.len());
    for (idx, _) in headers.iter().enumerate() {
        let raw = record.get(idx + 4).unwrap_or_default();
        let source_type = column_types
            .get(idx)
            .map(String::as_str)
            .unwrap_or("Nullable(Float64)");
        values.push(parse_scalar(raw, source_type)?);
    }

    Ok(StructuredRow {
        source_url: Some(source_url.to_string()),
        archive_entry: Some(csv_name.to_string()),
        values,
    })
}

fn parse_scalar(value: &str, source_type: &str) -> Result<RawValue> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(RawValue::Null);
    }
    if source_type.contains("String") {
        return Ok(RawValue::String(trimmed.to_string()));
    }
    if source_type.contains("DateTime") {
        let datetime = parse_nem_datetime(trimmed)
            .ok_or_else(|| anyhow!("invalid NEM datetime `{trimmed}`"))?;
        return Ok(RawValue::DateTime(datetime));
    }
    if source_type == "Date" || source_type == "Nullable(Date)" {
        let date =
            parse_nem_date(trimmed).ok_or_else(|| anyhow!("invalid NEM date `{trimmed}`"))?;
        return Ok(RawValue::Date(date));
    }
    if source_type.contains("Float") {
        if let Ok(integer) = trimmed.parse::<i64>() {
            return Ok(RawValue::Int64(integer));
        }
        let float = trimmed
            .parse::<f64>()
            .with_context(|| format!("invalid float `{trimmed}`"))?;
        return Ok(RawValue::Float64(float));
    }
    Ok(RawValue::String(trimmed.to_string()))
}

#[derive(Default)]
struct MaterializingRowSink {
    order: Vec<(String, String)>,
    rows_by_output: HashMap<(String, String), Vec<StructuredRow>>,
}

#[derive(Default)]
struct MaterializingEventSink {
    observed_schemas: Vec<ObservedSchema>,
    row_sink: MaterializingRowSink,
}

impl StructuredRawEventSink for MaterializingEventSink {
    fn accept(&mut self, event: StructuredRawEvent) -> Result<()> {
        match event {
            StructuredRawEvent::Schema(schema) => {
                self.observed_schemas.push(schema);
                Ok(())
            }
            StructuredRawEvent::Row(row) => self.row_sink.accept(row),
        }
    }
}

impl MaterializingEventSink {
    fn into_parse_result(self) -> ParseResult {
        let raw_outputs = self.row_sink.into_raw_outputs_from_order();
        ParseResult {
            observed_schemas: self.observed_schemas,
            raw_outputs,
            promotions: Vec::<PromotionMapping>::new(),
        }
    }
}

impl RawTableRowSink for MaterializingRowSink {
    fn accept(&mut self, row: RawTableRow) -> Result<()> {
        let key = (row.logical_table_key, row.schema_key);
        let entry = self.rows_by_output.entry(key.clone()).or_insert_with(|| {
            self.order.push(key);
            Vec::new()
        });
        entry.push(row.row);
        Ok(())
    }
}

impl MaterializingRowSink {
    fn into_raw_outputs_from_order(self) -> Vec<RawTableChunk> {
        let mut rows_by_output = self.rows_by_output;
        self.order
            .into_iter()
            .filter_map(|(logical_table_key, schema_key)| {
                let key = (logical_table_key.clone(), schema_key.clone());
                rows_by_output.remove(&key).map(|rows| RawTableChunk {
                    logical_table_key,
                    schema_key,
                    row_count: rows.len(),
                    rows,
                })
            })
            .collect()
    }
}
