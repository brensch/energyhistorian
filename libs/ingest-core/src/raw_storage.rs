use crate::schema::ObservedSchema;

#[derive(Debug, Clone)]
pub struct RawTablePlan {
    pub database: String,
    pub table_name: String,
    pub full_name: String,
    pub create_sql: String,
}

pub fn plan_raw_table(schema: &ObservedSchema) -> RawTablePlan {
    let database = "raw".to_string();
    let table_name = physical_raw_table_name(schema);
    let full_name = format!("{database}.{table_name}");

    let mut columns = Vec::new();
    columns.push("\"processed_at\" DateTime64(3) CODEC(Delta(8), LZ4)".to_string());
    columns.push("\"artifact_id\" String CODEC(ZSTD(6))".to_string());
    columns.push("\"source_id\" LowCardinality(String) CODEC(ZSTD(6))".to_string());
    columns.push("\"collection_id\" LowCardinality(String) CODEC(ZSTD(6))".to_string());
    columns.push("\"schema_hash\" LowCardinality(String) CODEC(ZSTD(6))".to_string());
    columns.push("\"source_url\" String CODEC(ZSTD(6))".to_string());
    columns.push("\"archive_entry\" String CODEC(ZSTD(6))".to_string());
    columns.push("\"row_hash\" UInt64 CODEC(ZSTD(6))".to_string());

    for column in &schema.columns {
        let name = quote_ident(&column.name);
        let column_type = column
            .source_data_type
            .as_deref()
            .unwrap_or("Nullable(String)");
        columns.push(format!(
            "{name} {column_type} CODEC({})",
            choose_codec(column_type)
        ));
    }

    let partition_col = choose_partition_column(schema);
    let order_by = choose_order_by(schema, partition_col.as_deref());
    let partition_clause = partition_col
        .map(|col| {
            let expr = partition_expression(schema, &col);
            format!("PARTITION BY toYYYYMM({expr})")
        })
        .unwrap_or_default();

    let create_sql = format!(
        "CREATE DATABASE IF NOT EXISTS {database};\n\
         CREATE TABLE IF NOT EXISTS {full_name} (\n  {}\n) \
         ENGINE = ReplacingMergeTree(processed_at) {} ORDER BY ({}) \
         SETTINGS compress_marks = true, compress_primary_key = true, allow_nullable_key = 1",
        columns.join(",\n  "),
        partition_clause,
        order_by
            .into_iter()
            .map(|column| quote_ident(&column))
            .collect::<Vec<_>>()
            .join(", ")
    );

    RawTablePlan {
        database,
        table_name,
        full_name,
        create_sql,
    }
}

pub fn physical_raw_table_name(schema: &ObservedSchema) -> String {
    let logical = &schema.schema_key.logical_table;
    let report_version = sanitize_part(&schema.schema_key.report_version);
    let hash_prefix = &schema.schema_key.header_hash[..12.min(schema.schema_key.header_hash.len())];

    format!(
        "{}__{}__{}__v{}__h_{}",
        sanitize_part(&logical.source_family),
        sanitize_part(&logical.section),
        sanitize_part(&logical.table),
        report_version,
        hash_prefix
    )
}

fn choose_partition_column(schema: &ObservedSchema) -> Option<String> {
    const PARTITION_CANDIDATES: &[&str] = &[
        "SETTLEMENTDATE",
        "INTERVAL_DATETIME",
        "DIRECTION_START_DATE",
        "EFFECTIVE_START_DATETIME",
        "MEASUREMENTTIME",
        "MEASUREMENT_DATETIME",
        "TRADINGDATE",
        "BIDSETTLEMENTDATE",
        "OFFERDATE",
    ];

    schema
        .columns
        .iter()
        .find(|column| PARTITION_CANDIDATES.contains(&column.name.as_str()))
        .map(|column| column.name.clone())
}

fn choose_order_by(schema: &ObservedSchema, partition_col: Option<&str>) -> Vec<String> {
    let mut result = Vec::new();
    if let Some(partition) = partition_col {
        result.push(partition.to_string());
    }

    for candidate in ["REGIONID", "DUID", "INTERCONNECTORID", "RUNNO", "PERIODID"] {
        if schema.columns.iter().any(|column| column.name == candidate)
            && !result.iter().any(|existing| existing == candidate)
        {
            result.push(candidate.to_string());
        }
    }

    result.push("artifact_id".to_string());
    result.push("row_hash".to_string());

    result
}

fn sanitize_part(value: &str) -> String {
    value
        .to_ascii_lowercase()
        .replace(['/', '-', '.', ' '], "_")
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

fn partition_expression(schema: &ObservedSchema, column_name: &str) -> String {
    let quoted = quote_ident(column_name);
    let source_type = schema
        .columns
        .iter()
        .find(|column| column.name == column_name)
        .and_then(|column| column.source_data_type.as_deref())
        .unwrap_or("Nullable(String)");

    if source_type.contains("DateTime") || source_type == "Date" || source_type == "Nullable(Date)"
    {
        quoted
    } else {
        format!("parseDateTimeBestEffortOrNull({quoted})")
    }
}

fn choose_codec(column_type: &str) -> &'static str {
    if column_type.contains("DateTime") {
        "Delta(8), LZ4"
    } else {
        "ZSTD(6)"
    }
}

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
