use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum RawValue {
    Null,
    String(String),
    Int64(i64),
    Float64(f64),
    Date(NaiveDate),
    DateTime(DateTime<Utc>),
}

impl RawValue {
    pub fn estimated_binary_size(&self) -> usize {
        match self {
            Self::Null => 1,
            Self::String(value) => leb128_len(value.len() as u64) + value.len(),
            Self::Int64(_) => 8,
            Self::Float64(_) => 8,
            Self::Date(_) => 2,
            Self::DateTime(_) => 8,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct StructuredRow {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archive_entry: Option<String>,
    #[serde(default)]
    pub values: Vec<RawValue>,
}

impl StructuredRow {
    pub fn estimated_binary_size(&self) -> usize {
        self.values
            .iter()
            .map(RawValue::estimated_binary_size)
            .sum::<usize>()
            + self
                .source_url
                .as_ref()
                .map(|value| leb128_len(value.len() as u64) + value.len())
                .unwrap_or(1)
            + self
                .archive_entry
                .as_ref()
                .map(|value| leb128_len(value.len() as u64) + value.len())
                .unwrap_or(leb128_len(0))
    }
}

fn leb128_len(mut value: u64) -> usize {
    let mut len = 1usize;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}
