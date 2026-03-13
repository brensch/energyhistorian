use anyhow::{Context, Result, bail};
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde_json::Value;
use url::Url;

use crate::models::{QueryPreview, SemanticObject};

#[derive(Clone)]
pub struct ClickHouseClient {
    read: HttpConn,
    write: HttpConn,
    view_db: String,
    usage_db: String,
}

#[derive(Clone)]
struct HttpConn {
    client: Client,
    url: String,
    user: String,
    password: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RegistryRow {
    source_id: String,
    object_name: String,
    object_kind: String,
    description: String,
    grain: String,
    time_column: String,
    dimensions: Vec<String>,
    measures: Vec<String>,
    join_keys: Vec<String>,
    caveats: Vec<String>,
    question_tags: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ReadonlyCheckRow {
    user: String,
    readonly: u8,
}

impl ClickHouseClient {
    pub fn new(read_url: &str, write_url: &str, view_db: String, usage_db: String) -> Result<Self> {
        Ok(Self {
            read: HttpConn::from_dsn(read_url)?,
            write: HttpConn::from_dsn(write_url)?,
            view_db,
            usage_db,
        })
    }

    pub async fn ensure_ready(&self) -> Result<()> {
        self.read.execute("SELECT 1").await?;
        self.ensure_read_conn_is_readonly().await
    }

    pub async fn ensure_usage_tables(&self) -> Result<()> {
        self.write
            .execute(&format!(
                "CREATE DATABASE IF NOT EXISTS {}",
                sanitize_ident(&self.usage_db)
            ))
            .await?;
        self.write
            .execute(&format!(
                "CREATE TABLE IF NOT EXISTS {}.ai_api_llm_usage (
                    event_time DateTime,
                    request_id String,
                    user_id String,
                    user_email String,
                    org_id String,
                    session_id String,
                    model String,
                    prompt_type String,
                    input_tokens UInt64,
                    output_tokens UInt64,
                    estimated_cost_usd Float64
                ) ENGINE = MergeTree ORDER BY (event_time, user_id, request_id)",
                sanitize_ident(&self.usage_db)
            ))
            .await?;
        self.write
            .execute(&format!(
                "CREATE TABLE IF NOT EXISTS {}.ai_api_query_usage (
                    event_time DateTime,
                    request_id String,
                    user_id String,
                    user_email String,
                    org_id String,
                    session_id String,
                    sql_text String,
                    rows_returned UInt64,
                    duration_ms UInt64,
                    succeeded UInt8,
                    error_text String
                ) ENGINE = MergeTree ORDER BY (event_time, user_id, request_id)",
                sanitize_ident(&self.usage_db)
            ))
            .await?;
        Ok(())
    }

    pub async fn load_registry(&self) -> Result<Vec<SemanticObject>> {
        let sql = format!(
            "SELECT source_id, object_name, object_kind, description, grain, time_column, dimensions, measures, join_keys, caveats, question_tags FROM {}.semantic_model_registry ORDER BY object_name",
            sanitize_ident(&self.view_db)
        );
        let rows = self.read.query_json_each_row::<RegistryRow>(&sql).await?;
        Ok(rows
            .into_iter()
            .map(|row| SemanticObject {
                source_id: row.source_id,
                object_name: row.object_name,
                object_kind: row.object_kind,
                description: row.description,
                grain: row.grain,
                time_column: row.time_column,
                dimensions: row.dimensions,
                measures: row.measures,
                join_keys: row.join_keys,
                caveats: row.caveats,
                question_tags: row.question_tags,
            })
            .collect())
    }

    pub async fn explain_syntax(&self, sql: &str) -> Result<()> {
        self.read
            .execute(&format!("EXPLAIN SYNTAX {}", sql.trim()))
            .await
    }

    pub async fn query_preview(&self, sql: &str) -> Result<QueryPreview> {
        let query = format!("{}\nFORMAT JSON", sql.trim().trim_end_matches(';'));
        let response = self.read.raw_query(&query).await?;
        if response.status() != StatusCode::OK {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            bail!("clickhouse returned {status}: {body}");
        }
        let payload: ClickHouseJsonResult = response.json().await?;
        let columns = payload
            .meta
            .into_iter()
            .map(|meta| meta.name)
            .collect::<Vec<_>>();
        let rows = payload
            .data
            .into_iter()
            .map(|row| match row {
                Value::Object(map) => columns
                    .iter()
                    .map(|column| map.get(column).cloned().unwrap_or(Value::Null))
                    .collect::<Vec<_>>(),
                Value::Array(values) => values,
                other => vec![other],
            })
            .collect::<Vec<_>>();
        Ok(QueryPreview {
            row_count: rows.len(),
            columns,
            rows,
        })
    }

    pub async fn log_llm_usage(
        &self,
        request_id: &str,
        user_id: &str,
        user_email: &str,
        org_id: &str,
        session_id: &str,
        model: &str,
        prompt_type: &str,
        input_tokens: i64,
        output_tokens: i64,
        estimated_cost_usd: f64,
    ) -> Result<()> {
        let row = serde_json::json!({
            "event_time": chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
            "request_id": request_id,
            "user_id": user_id,
            "user_email": user_email,
            "org_id": org_id,
            "session_id": session_id,
            "model": model,
            "prompt_type": prompt_type,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "estimated_cost_usd": estimated_cost_usd,
        });
        self.write
            .insert_json_each_row(&self.usage_db, "ai_api_llm_usage", &[row])
            .await
    }

    pub async fn log_query_usage(
        &self,
        request_id: &str,
        user_id: &str,
        user_email: &str,
        org_id: &str,
        session_id: &str,
        sql_text: &str,
        rows_returned: usize,
        duration_ms: u128,
        succeeded: bool,
        error_text: Option<&str>,
    ) -> Result<()> {
        let row = serde_json::json!({
            "event_time": chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
            "request_id": request_id,
            "user_id": user_id,
            "user_email": user_email,
            "org_id": org_id,
            "session_id": session_id,
            "sql_text": sql_text,
            "rows_returned": rows_returned,
            "duration_ms": duration_ms,
            "succeeded": if succeeded { 1 } else { 0 },
            "error_text": error_text.unwrap_or_default(),
        });
        self.write
            .insert_json_each_row(&self.usage_db, "ai_api_query_usage", &[row])
            .await
    }

    async fn ensure_read_conn_is_readonly(&self) -> Result<()> {
        let row = self
            .read
            .query_one_json_each_row::<ReadonlyCheckRow>(
                "SELECT currentUser() AS user, getSetting('readonly') AS readonly",
            )
            .await?;
        if row.readonly == 0 {
            bail!(
                "CLICKHOUSE_READ_URL must use a readonly ClickHouse user; currentUser()={} readonly={}",
                row.user,
                row.readonly
            );
        }
        Ok(())
    }
}

impl HttpConn {
    fn from_dsn(raw: &str) -> Result<Self> {
        let parsed = Url::parse(raw).with_context(|| format!("parsing clickhouse DSN {raw}"))?;
        let mut base = parsed.clone();
        if parsed.scheme() == "clickhouse" {
            base.set_scheme("http").ok();
            if parsed.port() == Some(9000) {
                base.set_port(Some(8123)).ok();
            }
        }
        base.set_path("/");
        base.set_query(None);
        Ok(Self {
            client: Client::builder().build()?,
            url: base.to_string(),
            user: parsed.username().to_string(),
            password: parsed.password().map(ToString::to_string),
        })
    }

    async fn query_json_each_row<T>(&self, sql: &str) -> Result<Vec<T>>
    where
        T: serde::de::DeserializeOwned,
    {
        let statement = format!("{}\nFORMAT JSONEachRow", sql.trim().trim_end_matches(';'));
        let response = self.raw_query(&statement).await?;
        if response.status() != StatusCode::OK {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            bail!("clickhouse returned {status}: {body}");
        }
        let body = response.text().await?;
        body.lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| serde_json::from_str(line).context("decoding JSONEachRow"))
            .collect()
    }

    async fn query_one_json_each_row<T>(&self, sql: &str) -> Result<T>
    where
        T: serde::de::DeserializeOwned,
    {
        let mut rows = self.query_json_each_row(sql).await?;
        if rows.len() != 1 {
            bail!("expected exactly one ClickHouse row, got {}", rows.len());
        }
        Ok(rows.remove(0))
    }

    async fn execute(&self, sql: &str) -> Result<()> {
        let response = self.raw_query(sql).await?;
        if response.status() != StatusCode::OK {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            bail!("clickhouse returned {status}: {body}");
        }
        Ok(())
    }

    async fn insert_json_each_row(
        &self,
        database: &str,
        table: &str,
        rows: &[Value],
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut body = format!(
            "INSERT INTO {}.{} FORMAT JSONEachRow\n",
            sanitize_ident(database),
            sanitize_ident(table)
        );
        for row in rows {
            body.push_str(&serde_json::to_string(row)?);
            body.push('\n');
        }
        self.execute(&body).await
    }

    async fn raw_query(&self, sql: &str) -> Result<reqwest::Response> {
        let mut request = self.client.post(&self.url).body(sql.to_string());
        if !self.user.is_empty() {
            request = request.basic_auth(&self.user, self.password.as_deref());
        }
        request.send().await.map_err(Into::into)
    }
}

#[derive(Debug, Deserialize)]
struct ClickHouseJsonResult {
    meta: Vec<ClickHouseColumnMeta>,
    data: Vec<Value>,
}

#[derive(Debug, Deserialize)]
struct ClickHouseColumnMeta {
    name: String,
}

fn sanitize_ident(value: &str) -> String {
    value.replace('"', "").replace('`', "")
}
