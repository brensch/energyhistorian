use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Datelike, NaiveDate, Utc};
use ingest_core::{
    LocalArtifact, ObservedSchema, ParseResult, RawPluginParseResult, RawValue, StructuredRow,
    plan_raw_table_in_database,
};
use reqwest::StatusCode;
use serde::de::DeserializeOwned;
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

        let processed_at = Utc::now();
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
                    processed_at,
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
        processed_at: DateTime<Utc>,
        rows: &[StructuredRow],
    ) -> Result<usize> {
        let plan = plan_raw_table_in_database(&raw_database_name(source_id), schema);
        let column_types = schema
            .columns
            .iter()
            .map(|column| {
                parse_clickhouse_type(
                    column
                        .source_data_type
                        .as_deref()
                        .unwrap_or("Nullable(String)"),
                )
                .with_context(|| format!("parsing type for column {}", column.name))
            })
            .collect::<Result<Vec<_>>>()?;
        let sql = format!(
            "INSERT INTO {} ({}) FORMAT RowBinary",
            plan.full_name,
            raw_insert_column_list(schema)
        );
        let capacity = rows
            .iter()
            .map(StructuredRow::estimated_binary_size)
            .sum::<usize>()
            + (rows.len() * 96);
        let mut body = Vec::with_capacity(capacity);
        for row in rows {
            encode_raw_row(
                &mut body,
                schema,
                source_id,
                collection_id,
                artifact_id,
                remote_uri,
                processed_at,
                row,
                &column_types,
            )?;
        }
        self.execute_binary_insert(&sql, body).await?;

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

    pub async fn query_json_rows<T>(&self, sql: &str) -> Result<Vec<T>>
    where
        T: DeserializeOwned,
    {
        let statement = format!("{sql}\nFORMAT JSONEachRow");
        let response = self
            .client
            .post(&self.config.url)
            .basic_auth(&self.config.user, Some(&self.config.password))
            .body(statement.clone())
            .send()
            .await
            .with_context(|| format!("sending clickhouse query: {}", summarize_sql(&statement)))?;
        if response.status() != StatusCode::OK {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            bail!(
                "clickhouse returned {} for `{}`: {}",
                status,
                summarize_sql(&statement),
                body
            );
        }
        let body = response.text().await?;
        body.lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| serde_json::from_str(line).context("decoding clickhouse JSONEachRow"))
            .collect()
    }

    pub async fn execute_sql(&self, sql: &str) -> Result<()> {
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

    async fn execute_binary_insert(&self, sql: &str, body: Vec<u8>) -> Result<()> {
        let response = self
            .client
            .post(&self.config.url)
            .basic_auth(&self.config.user, Some(&self.config.password))
            .query(&[("query", sql)])
            .body(body)
            .send()
            .await
            .with_context(|| format!("sending clickhouse binary insert: {}", summarize_sql(sql)))?;
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

pub fn raw_database_name(source_id: &str) -> String {
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

fn raw_insert_column_list(schema: &ObservedSchema) -> String {
    let mut columns = vec![
        quote_ident("processed_at"),
        quote_ident("artifact_id"),
        quote_ident("source_id"),
        quote_ident("collection_id"),
        quote_ident("schema_hash"),
        quote_ident("source_url"),
        quote_ident("archive_entry"),
        quote_ident("row_hash"),
    ];
    columns.extend(
        schema
            .columns
            .iter()
            .map(|column| quote_ident(&column.name)),
    );
    columns.join(", ")
}

fn encode_raw_row(
    buffer: &mut Vec<u8>,
    schema: &ObservedSchema,
    source_id: &str,
    collection_id: &str,
    artifact_id: &str,
    remote_uri: &str,
    processed_at: DateTime<Utc>,
    row: &StructuredRow,
    column_types: &[ClickHouseType],
) -> Result<()> {
    encode_datetime64(buffer, processed_at, 3);
    encode_string(buffer, artifact_id);
    encode_string(buffer, source_id);
    encode_string(buffer, collection_id);
    encode_string(buffer, &schema.schema_key.header_hash);
    encode_string(buffer, row.source_url.as_deref().unwrap_or(remote_uri));
    encode_string(buffer, row.archive_entry.as_deref().unwrap_or(""));
    buffer.extend_from_slice(&row_hash_value(row).to_le_bytes());

    for (idx, column) in schema.columns.iter().enumerate() {
        let value = row.values.get(idx).unwrap_or(&RawValue::Null);
        let field_type = column_types
            .get(idx)
            .ok_or_else(|| anyhow!("missing parsed type for column {}", column.name))?;
        encode_value(buffer, field_type, value)
            .with_context(|| format!("encoding rowbinary column {}", column.name))?;
    }

    Ok(())
}

fn row_hash_value(row: &StructuredRow) -> u64 {
    let mut hasher = Sha256::new();
    hasher.update((row.values.len() as u64).to_le_bytes());
    for value in &row.values {
        hash_raw_value(&mut hasher, value);
    }
    let digest = hasher.finalize();
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    u64::from_le_bytes(bytes)
}

fn hash_raw_value(hasher: &mut Sha256, value: &RawValue) {
    match value {
        RawValue::Null => hasher.update([0]),
        RawValue::String(text) => {
            hasher.update([1]);
            hasher.update((text.len() as u64).to_le_bytes());
            hasher.update(text.as_bytes());
        }
        RawValue::Int64(number) => {
            hasher.update([2]);
            hasher.update(number.to_le_bytes());
        }
        RawValue::Float64(number) => {
            hasher.update([3]);
            hasher.update(number.to_bits().to_le_bytes());
        }
        RawValue::Date(date) => {
            hasher.update([4]);
            hasher.update(date.num_days_from_ce().to_le_bytes());
        }
        RawValue::DateTime(datetime) => {
            hasher.update([5]);
            hasher.update(datetime.timestamp_millis().to_le_bytes());
        }
    }
}

#[derive(Debug, Clone)]
enum ClickHouseType {
    Nullable(Box<ClickHouseType>),
    String,
    Float64,
    Int64,
    UInt64,
    Date,
    DateTime64(u32),
}

fn parse_clickhouse_type(value: &str) -> Result<ClickHouseType> {
    let trimmed = value.trim();
    if let Some(inner) = trimmed
        .strip_prefix("Nullable(")
        .and_then(|rest| rest.strip_suffix(')'))
    {
        return Ok(ClickHouseType::Nullable(Box::new(parse_clickhouse_type(
            inner,
        )?)));
    }
    if let Some(inner) = trimmed
        .strip_prefix("LowCardinality(")
        .and_then(|rest| rest.strip_suffix(')'))
    {
        return parse_clickhouse_type(inner);
    }
    if trimmed == "String" {
        return Ok(ClickHouseType::String);
    }
    if trimmed == "Float64" {
        return Ok(ClickHouseType::Float64);
    }
    if trimmed == "Int64" {
        return Ok(ClickHouseType::Int64);
    }
    if trimmed == "UInt64" {
        return Ok(ClickHouseType::UInt64);
    }
    if trimmed == "Date" {
        return Ok(ClickHouseType::Date);
    }
    if let Some(scale) = parse_datetime64_scale(trimmed) {
        return Ok(ClickHouseType::DateTime64(scale));
    }
    bail!("unsupported ClickHouse type `{trimmed}` for RowBinary encoding")
}

fn parse_datetime64_scale(value: &str) -> Option<u32> {
    let inner = value.strip_prefix("DateTime64(")?.strip_suffix(')')?;
    let scale = inner.split(',').next()?.trim();
    scale.parse().ok()
}

fn encode_value(buffer: &mut Vec<u8>, field_type: &ClickHouseType, value: &RawValue) -> Result<()> {
    match field_type {
        ClickHouseType::Nullable(inner) => {
            if matches!(value, RawValue::Null) {
                buffer.push(1);
                return Ok(());
            }
            buffer.push(0);
            encode_value(buffer, inner, value)
        }
        ClickHouseType::String => {
            let text = match value {
                RawValue::Null => "",
                RawValue::String(text) => text.as_str(),
                RawValue::Int64(number) => return Ok(encode_string(buffer, &number.to_string())),
                RawValue::Float64(number) => return Ok(encode_string(buffer, &number.to_string())),
                RawValue::Date(date) => return Ok(encode_string(buffer, &date.to_string())),
                RawValue::DateTime(datetime) => {
                    return Ok(encode_string(buffer, &datetime.to_rfc3339()));
                }
            };
            encode_string(buffer, text);
            Ok(())
        }
        ClickHouseType::Float64 => {
            let number = match value {
                RawValue::Float64(number) => *number,
                RawValue::Int64(number) => *number as f64,
                RawValue::String(text) => text
                    .parse()
                    .with_context(|| format!("invalid float `{text}`"))?,
                other => bail!("cannot encode {other:?} as Float64"),
            };
            buffer.extend_from_slice(&number.to_le_bytes());
            Ok(())
        }
        ClickHouseType::Int64 => {
            let number = match value {
                RawValue::Int64(number) => *number,
                RawValue::String(text) => text
                    .parse()
                    .with_context(|| format!("invalid int `{text}`"))?,
                other => bail!("cannot encode {other:?} as Int64"),
            };
            buffer.extend_from_slice(&number.to_le_bytes());
            Ok(())
        }
        ClickHouseType::UInt64 => {
            let number = match value {
                RawValue::Int64(number) if *number >= 0 => *number as u64,
                RawValue::String(text) => text
                    .parse()
                    .with_context(|| format!("invalid uint `{text}`"))?,
                other => bail!("cannot encode {other:?} as UInt64"),
            };
            buffer.extend_from_slice(&number.to_le_bytes());
            Ok(())
        }
        ClickHouseType::Date => {
            let date = match value {
                RawValue::Date(date) => *date,
                RawValue::String(text) => NaiveDate::parse_from_str(text, "%Y-%m-%d")
                    .with_context(|| format!("invalid date `{text}`"))?,
                other => bail!("cannot encode {other:?} as Date"),
            };
            encode_date(buffer, date)?;
            Ok(())
        }
        ClickHouseType::DateTime64(scale) => {
            let datetime = match value {
                RawValue::DateTime(datetime) => *datetime,
                other => bail!("cannot encode {other:?} as DateTime64"),
            };
            encode_datetime64(buffer, datetime, *scale);
            Ok(())
        }
    }
}

fn encode_date(buffer: &mut Vec<u8>, value: NaiveDate) -> Result<()> {
    let origin = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let days = (value - origin).num_days();
    let encoded = u16::try_from(days).with_context(|| format!("date out of range `{value}`"))?;
    buffer.extend_from_slice(&encoded.to_le_bytes());
    Ok(())
}

fn encode_datetime64(buffer: &mut Vec<u8>, value: DateTime<Utc>, scale: u32) {
    let ticks = match scale {
        0 => value.timestamp(),
        3 => value.timestamp_millis(),
        6 => value.timestamp_micros(),
        9 => value
            .timestamp_nanos_opt()
            .unwrap_or_else(|| value.timestamp_micros() * 1_000),
        other => {
            let factor = 10_i64.pow(other);
            value.timestamp().saturating_mul(factor)
        }
    };
    buffer.extend_from_slice(&ticks.to_le_bytes());
}

fn encode_string(buffer: &mut Vec<u8>, value: &str) {
    put_leb128(buffer, value.len() as u64);
    buffer.extend_from_slice(value.as_bytes());
}

fn put_leb128(buffer: &mut Vec<u8>, mut value: u64) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        buffer.push(byte);
        if value == 0 {
            break;
        }
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

pub fn sanitize_identifier(value: &str) -> String {
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

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn summarize_sql(sql: &str) -> String {
    let single_line = sql.split_whitespace().collect::<Vec<_>>().join(" ");
    if single_line.len() > 120 {
        format!("{}...", &single_line[..120])
    } else {
        single_line
    }
}
