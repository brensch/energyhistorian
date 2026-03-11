use anyhow::{Context, Result, anyhow, bail};
use chrono::Utc;
use ingest_core::{
    LocalArtifact, ObservedSchema, ParseResult, RawPluginParseResult, plan_raw_table_in_database,
};
use reqwest::StatusCode;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

pub const MAX_INSERT_ROWS: usize = 10_000;
pub const MAX_INSERT_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct ClickHouseConfig {
    pub url: String,
    pub user: String,
    pub password: String,
}

#[derive(Clone)]
pub struct ClickHousePublisher {
    client: reqwest::Client,
    config: ClickHouseConfig,
}

impl ClickHousePublisher {
    pub fn new(config: ClickHouseConfig) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(60))
            .build()
            .context("building clickhouse HTTP client")?;
        Ok(Self { client, config })
    }

    pub async fn ensure_ready(&self) -> Result<()> {
        self.execute_sql("SELECT 1").await?;
        Ok(())
    }

    pub async fn publish_parse_result(
        &self,
        source_id: &str,
        collection_id: &str,
        artifact_id: &str,
        remote_uri: &str,
        result: &ParseResult,
    ) -> Result<usize> {
        let schemas_by_hash = result
            .observed_schemas
            .iter()
            .map(|schema| (schema.schema_key.header_hash.clone(), schema))
            .collect::<std::collections::HashMap<_, _>>();

        self.ensure_tables(source_id, collection_id, &result.observed_schemas)
            .await?;

        let processed_at = Utc::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string();
        let mut rows_written = 0usize;
        for chunk in &result.raw_outputs {
            let schema = schemas_by_hash
                .get(&chunk.schema_key)
                .copied()
                .ok_or_else(|| anyhow!("missing schema for chunk {}", chunk.schema_key))?;
            if chunk.rows.is_empty() {
                continue;
            }
            rows_written += self
                .publish_raw_chunk(
                    schema,
                    source_id,
                    collection_id,
                    artifact_id,
                    remote_uri,
                    &processed_at,
                    &chunk.rows,
                )
                .await?;
        }

        Ok(rows_written)
    }

    pub async fn publish_raw_plugin_parse_result(
        &self,
        source_id: &str,
        collection_id: &str,
        artifact: &LocalArtifact,
        result: &RawPluginParseResult,
    ) -> Result<usize> {
        let processed_at = Utc::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string();
        let mut rows_written = 0usize;
        for batch in &result.tables {
            if batch.rows.is_empty() {
                continue;
            }
            self.ensure_raw_plugin_table(source_id, &batch.table_name, &batch.rows)
                .await?;
            rows_written += self
                .insert_raw_plugin_rows(
                    source_id,
                    collection_id,
                    artifact,
                    &batch.table_name,
                    &processed_at,
                    &batch.rows,
                )
                .await?;
        }
        Ok(rows_written)
    }

    pub async fn ensure_tables(
        &self,
        source_id: &str,
        collection_id: &str,
        schemas: &[ObservedSchema],
    ) -> Result<()> {
        let database = raw_database_name(source_id);
        self.execute_sql(&format!("CREATE DATABASE IF NOT EXISTS {database}"))
            .await?;
        self.ensure_observed_schema_tables(&database).await?;
        for schema in schemas {
            let plan = plan_raw_table_in_database(&database, schema);
            for statement in plan
                .create_sql
                .split(';')
                .map(str::trim)
                .filter(|statement| !statement.is_empty())
            {
                self.execute_sql(statement).await?;
            }
            self.record_observed_schema(
                &database,
                source_id,
                collection_id,
                &plan.table_name,
                schema,
            )
            .await?;
        }
        Ok(())
    }

    async fn publish_raw_chunk(
        &self,
        schema: &ObservedSchema,
        source_id: &str,
        collection_id: &str,
        artifact_id: &str,
        remote_uri: &str,
        processed_at: &str,
        rows: &[Value],
    ) -> Result<usize> {
        let plan = plan_raw_table_in_database(&raw_database_name(source_id), schema);
        let prefix = format!("INSERT INTO {} FORMAT JSONEachRow\n", plan.full_name);
        let mut statement = prefix.clone();
        let mut rows_in_batch = 0usize;
        for row in rows {
            let line = build_insert_row(
                schema,
                source_id,
                collection_id,
                artifact_id,
                remote_uri,
                processed_at,
                row,
            )?;
            let encoded = serde_json::to_string(&line)?;
            let would_exceed_rows = rows_in_batch >= MAX_INSERT_ROWS;
            let would_exceed_bytes =
                rows_in_batch > 0 && statement.len() + encoded.len() + 1 > MAX_INSERT_BYTES;
            if would_exceed_rows || would_exceed_bytes {
                self.execute_sql(&statement).await?;
                statement.clear();
                statement.push_str(&prefix);
                rows_in_batch = 0;
            }
            statement.push_str(&encoded);
            statement.push('\n');
            rows_in_batch += 1;
        }

        if rows_in_batch > 0 {
            self.execute_sql(&statement).await?;
        }

        Ok(rows.len())
    }

    async fn ensure_raw_plugin_table(
        &self,
        source_id: &str,
        table_name: &str,
        rows: &[Value],
    ) -> Result<()> {
        let database = raw_database_name(source_id);
        self.execute_sql(&format!("CREATE DATABASE IF NOT EXISTS {database}"))
            .await?;
        let full_name = raw_plugin_table_name(source_id, table_name);
        self.execute_sql(&format!(
            r#"
            CREATE TABLE IF NOT EXISTS {full_name} (
              processed_at DateTime64(3) CODEC(Delta(8), LZ4),
              artifact_id String CODEC(ZSTD(6)),
              source_id LowCardinality(String) CODEC(ZSTD(6)),
              collection_id LowCardinality(String) CODEC(ZSTD(6)),
              source_url String CODEC(ZSTD(6)),
              local_path String CODEC(ZSTD(6)),
              content_sha256 String CODEC(ZSTD(6)),
              model_version Nullable(String) CODEC(ZSTD(6)),
              release_name Nullable(String) CODEC(ZSTD(6)),
              row_json String CODEC(ZSTD(6))
            )
            ENGINE = MergeTree
            ORDER BY (artifact_id, cityHash64(row_json))
            SETTINGS compress_marks = true, compress_primary_key = true
            "#
        ))
        .await?;

        for column_name in raw_plugin_column_names(rows) {
            self.execute_sql(&format!(
                "ALTER TABLE {full_name} ADD COLUMN IF NOT EXISTS `{column_name}` Nullable(String) CODEC(ZSTD(6))"
            ))
            .await?;
        }

        Ok(())
    }

    async fn ensure_observed_schema_tables(&self, database: &str) -> Result<()> {
        self.execute_sql(&format!(
            r#"
            CREATE TABLE IF NOT EXISTS {database}.observed_schemas (
              processed_at DateTime64(3) CODEC(Delta(8), LZ4),
              source_id LowCardinality(String) CODEC(ZSTD(6)),
              collection_id LowCardinality(String) CODEC(ZSTD(6)),
              physical_table LowCardinality(String) CODEC(ZSTD(6)),
              logical_source_family LowCardinality(String) CODEC(ZSTD(6)),
              logical_section LowCardinality(String) CODEC(ZSTD(6)),
              logical_table LowCardinality(String) CODEC(ZSTD(6)),
              report_version LowCardinality(String) CODEC(ZSTD(6)),
              model_version Nullable(String) CODEC(ZSTD(6)),
              schema_hash String CODEC(ZSTD(6)),
              observed_in_artifact_id String CODEC(ZSTD(6)),
              first_seen_at DateTime64(3) CODEC(Delta(8), LZ4),
              last_seen_at DateTime64(3) CODEC(Delta(8), LZ4),
              approval_status LowCardinality(String) CODEC(ZSTD(6)),
              column_count UInt32 CODEC(ZSTD(6))
            )
            ENGINE = MergeTree
            ORDER BY (logical_table, report_version, schema_hash)
            SETTINGS compress_marks = true, compress_primary_key = true
            "#
        ))
        .await?;
        self.execute_sql(&format!(
            r#"
            CREATE TABLE IF NOT EXISTS {database}.observed_schema_columns (
              processed_at DateTime64(3) CODEC(Delta(8), LZ4),
              source_id LowCardinality(String) CODEC(ZSTD(6)),
              collection_id LowCardinality(String) CODEC(ZSTD(6)),
              physical_table LowCardinality(String) CODEC(ZSTD(6)),
              logical_table LowCardinality(String) CODEC(ZSTD(6)),
              report_version LowCardinality(String) CODEC(ZSTD(6)),
              schema_hash String CODEC(ZSTD(6)),
              ordinal UInt32 CODEC(ZSTD(6)),
              column_name LowCardinality(String) CODEC(ZSTD(6)),
              source_data_type Nullable(String) CODEC(ZSTD(6)),
              nullable Nullable(UInt8) CODEC(ZSTD(6)),
              primary_key Nullable(UInt8) CODEC(ZSTD(6)),
              description Nullable(String) CODEC(ZSTD(6))
            )
            ENGINE = MergeTree
            ORDER BY (logical_table, schema_hash, ordinal)
            SETTINGS compress_marks = true, compress_primary_key = true
            "#
        ))
        .await?;
        Ok(())
    }

    async fn record_observed_schema(
        &self,
        database: &str,
        source_id: &str,
        collection_id: &str,
        physical_table: &str,
        schema: &ObservedSchema,
    ) -> Result<()> {
        let processed_at = Utc::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string();
        let schema_row = serde_json::to_string(&serde_json::json!({
            "processed_at": processed_at,
            "source_id": source_id,
            "collection_id": collection_id,
            "physical_table": physical_table,
            "logical_source_family": schema.schema_key.logical_table.source_family,
            "logical_section": schema.schema_key.logical_table.section,
            "logical_table": schema.schema_key.logical_table.table,
            "report_version": schema.schema_key.report_version,
            "model_version": schema.schema_key.model_version,
            "schema_hash": schema.schema_key.header_hash,
            "observed_in_artifact_id": schema.observed_in_artifact_id,
            "first_seen_at": schema.first_seen_at.format("%Y-%m-%d %H:%M:%S%.3f").to_string(),
            "last_seen_at": schema.last_seen_at.format("%Y-%m-%d %H:%M:%S%.3f").to_string(),
            "approval_status": format!("{:?}", schema.approval_status),
            "column_count": schema.columns.len() as u32,
        }))?;
        self.execute_sql(&format!(
            "INSERT INTO {database}.observed_schemas FORMAT JSONEachRow\n{schema_row}\n"
        ))
        .await?;

        if schema.columns.is_empty() {
            return Ok(());
        }

        let mut insert =
            format!("INSERT INTO {database}.observed_schema_columns FORMAT JSONEachRow\n");
        for column in &schema.columns {
            let row = serde_json::to_string(&serde_json::json!({
                "processed_at": processed_at,
                "source_id": source_id,
                "collection_id": collection_id,
                "physical_table": physical_table,
                "logical_table": schema.schema_key.logical_table.table,
                "report_version": schema.schema_key.report_version,
                "schema_hash": schema.schema_key.header_hash,
                "ordinal": column.ordinal as u32,
                "column_name": column.name,
                "source_data_type": column.source_data_type,
                "nullable": column.nullable.map(u8::from),
                "primary_key": column.primary_key.map(u8::from),
                "description": column.description,
            }))?;
            insert.push_str(&row);
            insert.push('\n');
        }
        self.execute_sql(&insert).await?;
        Ok(())
    }

    async fn insert_raw_plugin_rows(
        &self,
        source_id: &str,
        collection_id: &str,
        artifact: &LocalArtifact,
        table_name: &str,
        processed_at: &str,
        rows: &[Value],
    ) -> Result<usize> {
        let full_name = raw_plugin_table_name(source_id, table_name);
        let prefix = format!("INSERT INTO {full_name} FORMAT JSONEachRow\n");
        let mut statement = prefix.clone();
        let mut rows_in_batch = 0usize;
        for row in rows {
            let encoded = serde_json::to_string(&build_raw_plugin_row(
                source_id,
                collection_id,
                artifact,
                processed_at,
                row,
            )?)?;
            let would_exceed_rows = rows_in_batch >= MAX_INSERT_ROWS;
            let would_exceed_bytes =
                rows_in_batch > 0 && statement.len() + encoded.len() + 1 > MAX_INSERT_BYTES;
            if would_exceed_rows || would_exceed_bytes {
                self.execute_sql(&statement).await?;
                statement.clear();
                statement.push_str(&prefix);
                rows_in_batch = 0;
            }
            statement.push_str(&encoded);
            statement.push('\n');
            rows_in_batch += 1;
        }

        if rows_in_batch > 0 {
            self.execute_sql(&statement).await?;
        }

        Ok(rows.len())
    }

    async fn execute_sql(&self, sql: &str) -> Result<()> {
        let response = self
            .client
            .post(&self.config.url)
            .basic_auth(&self.config.user, Some(&self.config.password))
            .body(sql.to_string())
            .send()
            .await
            .with_context(|| format!("sending clickhouse SQL: {}", summarize_sql(sql)))?;
        if response.status() != StatusCode::OK {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            bail!(
                "clickhouse returned {} for `{}`: {}",
                status,
                summarize_sql(sql),
                body
            );
        }
        Ok(())
    }
}

fn raw_plugin_table_name(source_id: &str, table_name: &str) -> String {
    format!(
        "{}.{}",
        raw_database_name(source_id),
        sanitize_identifier(table_name)
    )
}

fn raw_database_name(source_id: &str) -> String {
    format!("raw_{}", sanitize_identifier(source_id))
}

fn raw_plugin_column_names(rows: &[Value]) -> Vec<String> {
    let mut names = rows
        .iter()
        .filter_map(Value::as_object)
        .flat_map(|object| object.keys())
        .map(|name| sanitize_identifier(name))
        .filter(|name| !is_fixed_raw_plugin_column(name))
        .collect::<Vec<_>>();
    names.sort();
    names.dedup();
    names
}

fn build_raw_plugin_row(
    source_id: &str,
    collection_id: &str,
    artifact: &LocalArtifact,
    processed_at: &str,
    row: &Value,
) -> Result<Map<String, Value>> {
    let object = row
        .as_object()
        .ok_or_else(|| anyhow!("raw plugin row payload must be an object"))?;
    let mut output = Map::new();
    output.insert(
        "processed_at".to_string(),
        Value::String(processed_at.to_string()),
    );
    output.insert(
        "artifact_id".to_string(),
        Value::String(artifact.metadata.artifact_id.clone()),
    );
    output.insert(
        "source_id".to_string(),
        Value::String(source_id.to_string()),
    );
    output.insert(
        "collection_id".to_string(),
        Value::String(collection_id.to_string()),
    );
    output.insert(
        "source_url".to_string(),
        Value::String(artifact.metadata.acquisition_uri.clone()),
    );
    output.insert(
        "local_path".to_string(),
        Value::String(artifact.local_path.display().to_string()),
    );
    output.insert(
        "content_sha256".to_string(),
        Value::String(artifact.metadata.content_sha256.clone().unwrap_or_default()),
    );
    output.insert(
        "model_version".to_string(),
        artifact
            .metadata
            .model_version
            .clone()
            .map(Value::String)
            .unwrap_or(Value::Null),
    );
    output.insert(
        "release_name".to_string(),
        artifact
            .metadata
            .release_name
            .clone()
            .map(Value::String)
            .unwrap_or(Value::Null),
    );
    output.insert(
        "row_json".to_string(),
        Value::String(serde_json::to_string(row)?),
    );
    for (name, value) in object {
        let column_name = sanitize_identifier(name);
        if is_fixed_raw_plugin_column(&column_name) {
            continue;
        }
        output.insert(column_name, stringify_raw_plugin_value(value)?);
    }
    Ok(output)
}

fn build_insert_row(
    schema: &ObservedSchema,
    source_id: &str,
    collection_id: &str,
    artifact_id: &str,
    remote_uri: &str,
    processed_at: &str,
    row: &Value,
) -> Result<Map<String, Value>> {
    let object = row
        .as_object()
        .ok_or_else(|| anyhow!("raw row payload must be an object"))?;
    let mut output = Map::new();
    output.insert(
        "processed_at".to_string(),
        Value::String(processed_at.to_string()),
    );
    output.insert(
        "artifact_id".to_string(),
        Value::String(artifact_id.to_string()),
    );
    output.insert(
        "source_id".to_string(),
        Value::String(source_id.to_string()),
    );
    output.insert(
        "collection_id".to_string(),
        Value::String(collection_id.to_string()),
    );
    output.insert(
        "schema_hash".to_string(),
        Value::String(schema.schema_key.header_hash.clone()),
    );
    output.insert(
        "source_url".to_string(),
        object
            .get("_source_url")
            .and_then(Value::as_str)
            .map(|value| Value::String(value.to_string()))
            .unwrap_or_else(|| Value::String(remote_uri.to_string())),
    );
    output.insert(
        "archive_entry".to_string(),
        object
            .get("_archive_entry")
            .and_then(Value::as_str)
            .map(|value| Value::String(value.to_string()))
            .unwrap_or_else(|| Value::String(String::new())),
    );
    output.insert(
        "row_hash".to_string(),
        Value::Number(serde_json::Number::from(row_hash_value(row)?)),
    );
    for column in &schema.columns {
        let source_type = column
            .source_data_type
            .as_deref()
            .unwrap_or("Nullable(String)");
        let source_key = column.name.to_ascii_lowercase();
        let value = object
            .get(&column.name)
            .or_else(|| object.get(&source_key))
            .cloned()
            .unwrap_or(Value::Null);
        output.insert(
            column.name.clone(),
            coerce_value(value, source_type)
                .with_context(|| format!("coercing column {}", column.name))?,
        );
    }
    Ok(output)
}

fn row_hash_value(row: &Value) -> Result<u64> {
    let canonical = serde_json::to_vec(row)?;
    let digest = Sha256::digest(canonical);
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    Ok(u64::from_le_bytes(bytes))
}

fn coerce_value(value: Value, source_type: &str) -> Result<Value> {
    if value.is_null() {
        return Ok(Value::Null);
    }
    if source_type.contains("String") {
        return Ok(Value::String(stringify_value(&value)?));
    }
    if source_type.contains("Float") {
        return match value {
            Value::Number(_) => Ok(value),
            Value::String(text) => {
                let parsed = text
                    .parse::<f64>()
                    .with_context(|| format!("invalid float `{text}`"))?;
                let number = serde_json::Number::from_f64(parsed)
                    .ok_or_else(|| anyhow!("non-finite float `{text}`"))?;
                Ok(Value::Number(number))
            }
            other => Err(anyhow!("cannot coerce {} to float", other)),
        };
    }
    if source_type.contains("DateTime") || source_type == "Date" || source_type == "Nullable(Date)"
    {
        return Ok(Value::String(stringify_value(&value)?));
    }
    Ok(value)
}

fn stringify_value(value: &Value) -> Result<String> {
    match value {
        Value::String(text) => Ok(text.clone()),
        Value::Number(number) => Ok(number.to_string()),
        Value::Bool(boolean) => Ok(boolean.to_string()),
        other => Err(anyhow!("cannot stringify {}", other)),
    }
}

fn stringify_raw_plugin_value(value: &Value) -> Result<Value> {
    Ok(match value {
        Value::Null => Value::Null,
        Value::String(text) => Value::String(text.clone()),
        Value::Number(number) => Value::String(number.to_string()),
        Value::Bool(boolean) => Value::String(boolean.to_string()),
        Value::Array(_) | Value::Object(_) => Value::String(serde_json::to_string(value)?),
    })
}

fn is_fixed_raw_plugin_column(column_name: &str) -> bool {
    matches!(
        column_name,
        "processed_at"
            | "artifact_id"
            | "source_id"
            | "collection_id"
            | "source_url"
            | "local_path"
            | "content_sha256"
            | "model_version"
            | "release_name"
            | "row_json"
    )
}

fn sanitize_identifier(value: &str) -> String {
    let mut sanitized = value
        .to_ascii_lowercase()
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect::<String>();
    sanitized = sanitized.trim_matches('_').to_string();
    if sanitized.is_empty() {
        return "field".to_string();
    }
    if sanitized
        .chars()
        .next()
        .is_some_and(|ch| ch.is_ascii_digit())
    {
        sanitized.insert_str(0, "f_");
    }
    sanitized
}

fn summarize_sql(sql: &str) -> String {
    let single_line = sql.split_whitespace().collect::<Vec<_>>().join(" ");
    if single_line.len() > 120 {
        format!("{}...", &single_line[..120])
    } else {
        single_line
    }
}
