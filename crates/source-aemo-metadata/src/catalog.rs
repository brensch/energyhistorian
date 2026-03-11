use serde::{Deserialize, Serialize};

/// An MMS package (e.g., DISPATCH, TRADING_DATA, PARTICIPANT_REGISTRATION).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MmsPackage {
    pub package_id: String,
    pub table_count: usize,
}

/// A table definition from the MMS Data Model.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MmsTable {
    pub package_id: String,
    pub table_name: String,
    /// Description directly from the MMS Data Model HTML.
    pub description: String,
    /// Source/update trigger notes directly from the MMS Data Model.
    pub source_notes: String,
    /// Volume notes directly from the MMS Data Model (e.g., "288 records per day").
    pub volume_notes: String,
    /// Visibility classification from MMS Data Model (Public, Private, etc.).
    pub visibility: String,
    /// Additional notes from the MMS Data Model.
    pub additional_notes: String,
    pub primary_key_columns: Vec<String>,
    pub index_columns: Vec<String>,
    pub columns: Vec<MmsColumn>,
}

/// A column definition from the MMS Data Model.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MmsColumn {
    pub column_name: String,
    pub ordinal: usize,
    /// Oracle data type as specified in the MMS Data Model (e.g., VARCHAR2(20), NUMBER(15,5), DATE).
    pub oracle_type: String,
    pub is_mandatory: bool,
    pub is_primary_key: bool,
    /// Column description/comment directly from the MMS Data Model.
    pub description: String,
    /// Unit extracted from description if present (MW, MWh, $/MWh, %).
    pub units: Option<String>,
    /// Valid values extracted from description if present (e.g., "0=normal / 1=Supply Scarcity").
    pub valid_values: Option<String>,
}

/// Maps a natural language concept to the correct table/column.
/// Derived from the MMS Data Model structure and table descriptions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataConcept {
    pub concept: String,
    pub natural_language_triggers: Vec<String>,
    pub primary_table: String,
    pub primary_column: Option<String>,
    pub alternative_table: Option<String>,
    pub alternative_notes: Option<String>,
    pub join_tables: Vec<String>,
    pub join_keys: Vec<String>,
    pub resolution: String,
    pub units: String,
    pub notes: String,
    pub formula: Option<String>,
}

/// Reference data for FCAS markets — the 10 FCAS markets and their column locations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FcasMarket {
    pub market_id: String,
    pub display_name: String,
    pub direction: String,
    pub response_time: String,
    pub price_column: String,
    pub price_table: String,
    pub volume_column: String,
    pub volume_table: String,
    pub enablement_column: String,
    pub enablement_table: String,
}

/// Reference data for interconnectors.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InterconnectorRef {
    pub interconnector_id: String,
    pub common_name: String,
    pub from_region: String,
    pub to_region: String,
    pub is_mnsp: bool,
}

/// The complete parsed catalog from the MMS Data Model + reference data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AemoCatalog {
    pub packages: Vec<MmsPackage>,
    pub tables: Vec<MmsTable>,
    pub concepts: Vec<DataConcept>,
    pub fcas_markets: Vec<FcasMarket>,
    pub interconnectors: Vec<InterconnectorRef>,
}
