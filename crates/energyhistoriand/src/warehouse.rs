use anyhow::{Context, Result, anyhow, bail};
use chrono::Utc;
use ingest_core::{ObservedSchema, ParseResult, plan_raw_table};
use reqwest::StatusCode;
use serde_json::{Map, Value};

const MAX_INSERT_ROWS: usize = 10_000;
const MAX_INSERT_BYTES: usize = 8 * 1024 * 1024;

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
        self.execute_sql("CREATE DATABASE IF NOT EXISTS raw")
            .await?;
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

        for schema in &result.observed_schemas {
            self.ensure_table(schema).await?;
        }

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
            self.insert_chunk(
                schema,
                source_id,
                collection_id,
                artifact_id,
                remote_uri,
                &processed_at,
                &chunk.rows,
            )
            .await?;
            rows_written += chunk.rows.len();
        }

        Ok(rows_written)
    }

    async fn ensure_table(&self, schema: &ObservedSchema) -> Result<()> {
        let plan = plan_raw_table(schema);
        for statement in plan
            .create_sql
            .split(';')
            .map(str::trim)
            .filter(|statement| !statement.is_empty())
        {
            self.execute_sql(statement).await?;
        }
        Ok(())
    }

    async fn insert_chunk(
        &self,
        schema: &ObservedSchema,
        source_id: &str,
        collection_id: &str,
        artifact_id: &str,
        remote_uri: &str,
        processed_at: &str,
        rows: &[Value],
    ) -> Result<()> {
        let plan = plan_raw_table(schema);
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
        Ok(())
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

fn summarize_sql(sql: &str) -> String {
    let single_line = sql.split_whitespace().collect::<Vec<_>>().join(" ");
    if single_line.len() > 120 {
        format!("{}...", &single_line[..120])
    } else {
        single_line
    }
}

#[cfg(test)]
mod tests {
    use super::{ClickHouseConfig, ClickHousePublisher};
    use chrono::Utc;
    use ingest_core::{
        LogicalTableId, ObservedSchema, ParseResult, RawTableChunk, SchemaApprovalStatus,
        SchemaColumn, SchemaVersionKey, physical_raw_table_name,
    };
    use serde_json::json;

    #[tokio::test]
    #[ignore = "requires running clickhouse"]
    async fn publishes_rows_to_clickhouse() {
        let config = ClickHouseConfig {
            url: std::env::var("CLICKHOUSE_URL")
                .unwrap_or_else(|_| "http://127.0.0.1:8123".to_string()),
            user: std::env::var("CLICKHOUSE_USER")
                .unwrap_or_else(|_| "energyhistorian".to_string()),
            password: std::env::var("CLICKHOUSE_PASSWORD")
                .unwrap_or_else(|_| "energyhistorian".to_string()),
        };
        let publisher = ClickHousePublisher::new(config.clone()).expect("publisher");
        publisher.ensure_ready().await.expect("clickhouse ready");

        let suffix = Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or_default()
            .to_string();
        let schema = ObservedSchema {
            schema_id: format!("test-schema-{suffix}"),
            schema_key: SchemaVersionKey {
                logical_table: LogicalTableId {
                    source_family: "AEMO.NEMWEB".to_string(),
                    section: "TEST".to_string(),
                    table: "PRICE".to_string(),
                },
                report_version: "1".to_string(),
                model_version: Some("test".to_string()),
                header_hash: format!("abc123def456{suffix}"),
            },
            first_seen_at: Utc::now(),
            last_seen_at: Utc::now(),
            observed_in_artifact_id: format!("artifact-{suffix}"),
            columns: vec![
                SchemaColumn {
                    ordinal: 1,
                    name: "SETTLEMENTDATE".to_string(),
                    source_data_type: Some("Nullable(DateTime64(3))".to_string()),
                    nullable: Some(true),
                    primary_key: None,
                    description: None,
                },
                SchemaColumn {
                    ordinal: 2,
                    name: "REGIONID".to_string(),
                    source_data_type: Some("String".to_string()),
                    nullable: Some(false),
                    primary_key: None,
                    description: None,
                },
                SchemaColumn {
                    ordinal: 3,
                    name: "RRP".to_string(),
                    source_data_type: Some("Nullable(Float64)".to_string()),
                    nullable: Some(true),
                    primary_key: None,
                    description: None,
                },
            ],
            approval_status: SchemaApprovalStatus::Proposed,
        };

        let parse_result = ParseResult {
            observed_schemas: vec![schema.clone()],
            raw_outputs: vec![RawTableChunk {
                logical_table_key: "AEMO.NEMWEB/TEST/PRICE/1".to_string(),
                schema_key: schema.schema_key.header_hash.clone(),
                rows: vec![
                    json!({
                        "_archive_entry": "sample.csv",
                        "_source_url": "https://example.test/source.zip",
                        "settlementdate": "2026-03-10 00:05:00",
                        "regionid": "NSW1",
                        "rrp": 123.45
                    }),
                    json!({
                        "_archive_entry": "sample.csv",
                        "_source_url": "https://example.test/source.zip",
                        "settlementdate": "2026-03-10 00:10:00",
                        "regionid": "QLD1",
                        "rrp": 67.89
                    }),
                ],
            }],
            promotions: Vec::new(),
        };

        let inserted = publisher
            .publish_parse_result(
                "aemo.nemweb",
                "test_collection",
                &format!("artifact-{suffix}"),
                "https://example.test/source.zip",
                &parse_result,
            )
            .await
            .expect("publish rows");
        assert_eq!(inserted, 2);

        let table_name = physical_raw_table_name(&schema);
        let response = reqwest::Client::new()
            .post(&config.url)
            .basic_auth(&config.user, Some(&config.password))
            .body(format!("SELECT count() FROM raw.{table_name}"))
            .send()
            .await
            .expect("query clickhouse");
        assert!(response.status().is_success());
        let count = response.text().await.expect("count body");
        assert_eq!(count.trim(), "2");
    }
}
