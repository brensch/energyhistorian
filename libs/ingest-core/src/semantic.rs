use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum SemanticNamingStrategy {
    Default,
    StripYearTokens,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SemanticJob {
    ConsolidateObservedSchemaViews {
        target_database: String,
        include_latest_alias: bool,
        naming_strategy: SemanticNamingStrategy,
    },
    SqlView {
        target_database: String,
        view_name: String,
        required_objects: Vec<String>,
        sql: String,
    },
    SqlTable {
        target_database: String,
        table_name: String,
        create_sql: String,
        populate_sql: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticModel {
    pub source_id: String,
    pub object_name: String,
    pub object_kind: String,
    pub description: String,
    pub grain: String,
    pub time_column: Option<String>,
    pub dimensions: Vec<String>,
    pub measures: Vec<String>,
    pub join_keys: Vec<String>,
    pub caveats: Vec<String>,
    pub question_tags: Vec<String>,
}

pub fn semantic_model_registry_sql(models: &[SemanticModel]) -> String {
    if models.is_empty() {
        return concat!(
            "SELECT CAST('', 'String') AS source_id, ",
            "CAST('', 'String') AS object_name, ",
            "CAST('', 'String') AS object_kind, ",
            "CAST('', 'String') AS description, ",
            "CAST('', 'String') AS grain, ",
            "CAST(NULL, 'Nullable(String)') AS time_column, ",
            "CAST([], 'Array(String)') AS dimensions, ",
            "CAST([], 'Array(String)') AS measures, ",
            "CAST([], 'Array(String)') AS join_keys, ",
            "CAST([], 'Array(String)') AS caveats, ",
            "CAST([], 'Array(String)') AS question_tags ",
            "WHERE 0"
        )
        .to_string();
    }

    models
        .iter()
        .map(|model| {
            format!(
                concat!(
                    "SELECT {source_id} AS source_id, ",
                    "{object_name} AS object_name, ",
                    "{object_kind} AS object_kind, ",
                    "{description} AS description, ",
                    "{grain} AS grain, ",
                    "{time_column} AS time_column, ",
                    "{dimensions} AS dimensions, ",
                    "{measures} AS measures, ",
                    "{join_keys} AS join_keys, ",
                    "{caveats} AS caveats, ",
                    "{question_tags} AS question_tags"
                ),
                source_id = sql_string(&model.source_id),
                object_name = sql_string(&model.object_name),
                object_kind = sql_string(&model.object_kind),
                description = sql_string(&model.description),
                grain = sql_string(&model.grain),
                time_column = sql_nullable_string(model.time_column.as_deref()),
                dimensions = sql_string_array(&model.dimensions),
                measures = sql_string_array(&model.measures),
                join_keys = sql_string_array(&model.join_keys),
                caveats = sql_string_array(&model.caveats),
                question_tags = sql_string_array(&model.question_tags),
            )
        })
        .collect::<Vec<_>>()
        .join(" UNION ALL ")
}

fn sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn sql_nullable_string(value: Option<&str>) -> String {
    value
        .map(sql_string)
        .unwrap_or_else(|| "CAST(NULL, 'Nullable(String)')".to_string())
}

fn sql_string_array(values: &[String]) -> String {
    if values.is_empty() {
        "CAST([], 'Array(String)')".to_string()
    } else {
        format!(
            "[{}]",
            values
                .iter()
                .map(|value| sql_string(value))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}
