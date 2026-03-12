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
