use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};

use anyhow::{Context, Result};
use ingest_core::{SemanticDedupeRule, SemanticJob, SemanticNamingStrategy};
use serde::{Deserialize, Deserializer};

use crate::clickhouse::{ClickHousePublisher, raw_database_name, sanitize_identifier};
use crate::source_registry::SourceRegistry;

#[derive(Debug, Deserialize)]
struct ObservedSchemaRow {
    logical_section: String,
    logical_table: String,
    report_version: String,
    physical_table: String,
    last_seen_at: String,
}

#[derive(Debug, Deserialize)]
struct ColumnRow {
    table: String,
    name: String,
    #[serde(rename = "type")]
    column_type: String,
    #[serde(deserialize_with = "deserialize_u64")]
    position: u64,
}

#[derive(Debug, Deserialize)]
struct CountRow {
    #[serde(deserialize_with = "deserialize_u64")]
    matched: u64,
}

#[derive(Debug, Clone)]
struct VersionGroup {
    report_version: String,
    physical_tables: Vec<String>,
    last_seen_at: String,
}

pub async fn reconcile_source_semantics(
    publisher: &ClickHousePublisher,
    registry: &SourceRegistry,
    source_id: &str,
) -> Result<usize> {
    let _guard = publisher.lock_semantic_reconcile().await;
    let jobs = registry.semantic_jobs(source_id)?;
    if jobs.is_empty() {
        return Ok(0);
    }

    publisher
        .execute_sql("CREATE DATABASE IF NOT EXISTS semantic")
        .await?;

    let mut executed = 0usize;
    for job in jobs {
        executed += reconcile_job(publisher, source_id, &job).await?;
    }
    Ok(executed)
}

async fn reconcile_job(
    publisher: &ClickHousePublisher,
    source_id: &str,
    job: &SemanticJob,
) -> Result<usize> {
    match job {
        SemanticJob::ConsolidateObservedSchemaViews {
            target_database,
            include_latest_alias,
            naming_strategy,
            dedupe_rules,
        } => {
            reconcile_observed_schema_views(
                publisher,
                source_id,
                target_database,
                *include_latest_alias,
                *naming_strategy,
                dedupe_rules,
            )
            .await
        }
        SemanticJob::SqlView {
            target_database,
            view_name,
            required_objects,
            sql,
        } => {
            if !required_objects.is_empty()
                && !required_objects_available(publisher, required_objects).await?
            {
                return Ok(0);
            }
            replace_view(publisher, &qualified_name(target_database, view_name), sql).await?;
            Ok(1)
        }
        SemanticJob::SqlTable {
            target_database,
            table_name,
            create_sql,
            populate_sql,
        } => {
            let full_name = qualified_name(target_database, table_name);
            publisher
                .execute_sql(&format!("DROP TABLE IF EXISTS {full_name}"))
                .await?;
            publisher
                .execute_sql(&format!("CREATE TABLE {full_name} {create_sql}"))
                .await?;
            publisher
                .execute_sql(&format!("INSERT INTO {full_name} {populate_sql}"))
                .await?;
            Ok(1)
        }
    }
}

async fn reconcile_observed_schema_views(
    publisher: &ClickHousePublisher,
    source_id: &str,
    target_database: &str,
    include_latest_alias: bool,
    naming_strategy: SemanticNamingStrategy,
    dedupe_rules: &[SemanticDedupeRule],
) -> Result<usize> {
    let raw_database = raw_database_name(source_id);
    let observed = publisher
        .query_json_rows::<ObservedSchemaRow>(&format!(
            "SELECT logical_section, logical_table, report_version, physical_table, max(last_seen_at) AS last_seen_at \
             FROM {raw_database}.observed_schemas \
             WHERE physical_table != '' \
             GROUP BY logical_section, logical_table, report_version, physical_table"
        ))
        .await
        .with_context(|| format!("loading observed schemas for {source_id}"))?;

    let mut groups: BTreeMap<(String, String), Vec<VersionGroup>> = BTreeMap::new();
    for row in observed {
        let key = (row.logical_section.clone(), row.logical_table.clone());
        let versions = groups.entry(key).or_default();
        if let Some(existing) = versions
            .iter_mut()
            .find(|group| group.report_version == row.report_version)
        {
            existing.physical_tables.push(row.physical_table);
            if row.last_seen_at > existing.last_seen_at {
                existing.last_seen_at = row.last_seen_at;
            }
        } else {
            versions.push(VersionGroup {
                report_version: row.report_version,
                physical_tables: vec![row.physical_table],
                last_seen_at: row.last_seen_at,
            });
        }
    }

    let dedupe_rules_by_view = dedupe_rules
        .iter()
        .map(|rule| (rule.view_name.as_str(), rule.key_columns.as_slice()))
        .collect::<HashMap<_, _>>();

    let mut executed = 0usize;
    for ((logical_section, logical_table), versions) in groups {
        let base_name = semantic_view_base_name(&logical_section, &logical_table, naming_strategy);
        let mut sorted_versions = versions;
        sorted_versions
            .sort_by(|left, right| compare_versions(&left.report_version, &right.report_version));

        let mut latest_view_name = None;
        for version in &sorted_versions {
            let view_name = if version.report_version.trim().is_empty() {
                base_name.clone()
            } else {
                format!(
                    "{}_v{}",
                    base_name,
                    sanitize_identifier(&version.report_version)
                )
            };
            let union_sql = build_union_view_sql(
                publisher,
                &raw_database,
                &version.physical_tables,
                dedupe_rules_by_view.get(base_name.as_str()).copied(),
            )
            .await?;
            replace_view(
                publisher,
                &qualified_name(target_database, &view_name),
                &union_sql,
            )
            .await?;
            executed += 1;
            latest_view_name = Some(view_name);
        }

        if include_latest_alias
            && let Some(latest_view_name) = latest_view_name
            && latest_view_name != base_name
        {
            replace_view(
                publisher,
                &qualified_name(target_database, &base_name),
                &format!(
                    "SELECT * FROM {}",
                    qualified_name(target_database, &latest_view_name)
                ),
            )
            .await?;
            executed += 1;
        }
    }

    Ok(executed)
}

async fn build_union_view_sql(
    publisher: &ClickHousePublisher,
    raw_database: &str,
    physical_tables: &[String],
    dedupe_key_columns: Option<&[String]>,
) -> Result<String> {
    let mut physical_tables = physical_tables.to_vec();
    physical_tables.sort();
    physical_tables.dedup();
    let quoted_tables = physical_tables
        .iter()
        .map(|table| format!("'{}'", table.replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(", ");
    let columns = publisher
        .query_json_rows::<ColumnRow>(&format!(
            "SELECT table, name, type, toUInt64(position) AS position \
             FROM system.columns \
             WHERE database = '{raw_database}' AND table IN ({quoted_tables})"
        ))
        .await
        .with_context(|| format!("loading column metadata for {raw_database}"))?;

    let mut columns_by_table: HashMap<String, Vec<ColumnRow>> = HashMap::new();
    for column in columns {
        columns_by_table
            .entry(column.table.clone())
            .or_default()
            .push(column);
    }
    for table_columns in columns_by_table.values_mut() {
        table_columns.sort_by_key(|column| column.position);
    }

    let mut preferred_order = Vec::new();
    let mut preferred_types = HashMap::new();
    for table in &physical_tables {
        if let Some(table_columns) = columns_by_table.get(table) {
            for column in table_columns {
                if preferred_types.contains_key(&column.name) {
                    continue;
                }
                preferred_order.push(column.name.clone());
                preferred_types.insert(column.name.clone(), nullable_type(&column.column_type));
            }
        }
    }

    let table_column_counts = columns_by_table
        .iter()
        .map(|(table, table_columns)| (table.as_str(), table_columns.len()))
        .collect::<HashMap<_, _>>();

    let selects = physical_tables
        .iter()
        .map(|table| {
            let table_columns = columns_by_table
                .get(table)
                .context("missing column metadata for physical table")?;
            let column_types = table_columns
                .iter()
                .map(|column| (column.name.as_str(), column.column_type.as_str()))
                .collect::<HashMap<_, _>>();
            let expressions = preferred_order
                .iter()
                .map(|column_name| {
                    let target_type = preferred_types
                        .get(column_name)
                        .expect("preferred type exists for ordered column");
                    if column_types.contains_key(column_name.as_str()) {
                        format!(
                            "CAST(`{column_name}`, {}) AS `{column_name}`",
                            sql_string_literal(target_type)
                        )
                    } else {
                        format!(
                            "CAST(NULL, {}) AS `{column_name}`",
                            sql_string_literal(target_type)
                        )
                    }
                })
                .collect::<Vec<_>>()
                .join(", ");
            Ok(format!(
                "SELECT {expressions}, toUInt32({column_count}) AS `_source_column_count` FROM {raw_database}.{}",
                sanitize_identifier(table),
                column_count = table_column_counts
                    .get(table.as_str())
                    .copied()
                    .unwrap_or(table_columns.len()),
            ))
        })
        .collect::<Result<Vec<_>>>()?;

    let union_sql = selects.join(" UNION ALL ");
    if let Some(key_columns) = dedupe_key_columns
        && !key_columns.is_empty()
        && key_columns
            .iter()
            .all(|column| preferred_order.iter().any(|candidate| candidate == column))
    {
        let projected_columns = preferred_order
            .iter()
            .map(|column| format!("`{column}`"))
            .collect::<Vec<_>>()
            .join(", ");
        let partition_by = key_columns
            .iter()
            .map(|column| format!("`{column}`"))
            .collect::<Vec<_>>()
            .join(", ");
        return Ok(format!(
            "SELECT {projected_columns} \
                 FROM ( \
                     SELECT {projected_columns}, row_number() OVER (PARTITION BY {partition_by} ORDER BY `_source_column_count` DESC, processed_at DESC, artifact_id DESC) AS _dedupe_rank \
                     FROM ({union_sql}) \
                 ) \
                 WHERE _dedupe_rank = 1"
        ));
    }

    if preferred_order.iter().any(|column| column == "row_hash") {
        let projected_columns = preferred_order
            .iter()
            .map(|column| format!("`{column}`"))
            .collect::<Vec<_>>()
            .join(", ");
        return Ok(format!(
            "SELECT {projected_columns} \
             FROM ( \
                 SELECT {projected_columns}, row_number() OVER (PARTITION BY row_hash ORDER BY processed_at DESC, artifact_id DESC) AS _dedupe_rank \
                 FROM ({union_sql}) \
             ) \
             WHERE _dedupe_rank = 1"
        ));
    }

    Ok(union_sql)
}

async fn replace_view(publisher: &ClickHousePublisher, full_name: &str, sql: &str) -> Result<()> {
    publisher
        .execute_sql(&format!("CREATE OR REPLACE VIEW {full_name} AS {sql}"))
        .await?;
    Ok(())
}

async fn required_objects_available(
    publisher: &ClickHousePublisher,
    required_objects: &[String],
) -> Result<bool> {
    for object_name in required_objects {
        let Some((database, name)) = parse_qualified_name(object_name) else {
            continue;
        };
        let rows = publisher
            .query_json_rows::<CountRow>(&format!(
                "SELECT count() AS matched FROM system.tables WHERE database = '{}' AND name = '{}'",
                database.replace('\'', "''"),
                name.replace('\'', "''")
            ))
            .await?;
        if rows.first().map(|row| row.matched).unwrap_or(0) == 0 {
            return Ok(false);
        }
    }
    Ok(true)
}

fn semantic_view_base_name(
    logical_section: &str,
    logical_table: &str,
    naming_strategy: SemanticNamingStrategy,
) -> String {
    let section = normalize_identifier_component(logical_section, naming_strategy);
    let table = normalize_identifier_component(logical_table, naming_strategy);
    if table.is_empty() || table == "field" {
        return section;
    }
    if section.is_empty() || section == "field" {
        table
    } else {
        sanitize_identifier(&format!("{section}_{table}"))
    }
}

fn normalize_identifier_component(value: &str, naming_strategy: SemanticNamingStrategy) -> String {
    let sanitized = sanitize_identifier(value);
    if !matches!(naming_strategy, SemanticNamingStrategy::StripYearTokens) {
        return sanitized;
    }

    let filtered = sanitized
        .split('_')
        .filter(|part| !is_year_token(part))
        .collect::<Vec<_>>();
    if filtered.is_empty() {
        sanitized
    } else {
        filtered.join("_")
    }
}

fn is_year_token(value: &str) -> bool {
    value.len() == 4
        && value.chars().all(|ch| ch.is_ascii_digit())
        && matches!(value.parse::<u16>(), Ok(1900..=2099))
}

fn qualified_name(database: &str, object_name: &str) -> String {
    format!(
        "{}.{}",
        sanitize_identifier(database),
        sanitize_identifier(object_name)
    )
}

fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn parse_qualified_name(value: &str) -> Option<(String, String)> {
    let (database, name) = value.split_once('.')?;
    Some((sanitize_identifier(database), sanitize_identifier(name)))
}

fn nullable_type(column_type: &str) -> String {
    if column_type.starts_with("Nullable(") {
        return column_type.to_string();
    }
    if let Some(inner) = column_type
        .strip_prefix("LowCardinality(")
        .and_then(|value| value.strip_suffix(')'))
    {
        if inner.starts_with("Nullable(") {
            return inner.to_string();
        }
        return format!("Nullable({inner})");
    }
    format!("Nullable({column_type})")
}

fn compare_versions(left: &str, right: &str) -> Ordering {
    let left_parts = version_parts(left);
    let right_parts = version_parts(right);
    left_parts.cmp(&right_parts).then_with(|| left.cmp(right))
}

fn version_parts(value: &str) -> Vec<u32> {
    let parts = value
        .split(|ch: char| !ch.is_ascii_digit())
        .filter(|part| !part.is_empty())
        .filter_map(|part| part.parse::<u32>().ok())
        .collect::<Vec<_>>();
    if parts.is_empty() { vec![0] } else { parts }
}

fn deserialize_u64<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum NumberOrString {
        Number(u64),
        String(String),
    }

    match NumberOrString::deserialize(deserializer)? {
        NumberOrString::Number(value) => Ok(value),
        NumberOrString::String(value) => value.parse::<u64>().map_err(serde::de::Error::custom),
    }
}
