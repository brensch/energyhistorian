use crate::nem_time::{
    looks_like_nem_date, looks_like_nem_datetime, parse_nem_date, parse_nem_datetime,
};

const MAX_ROWS_TO_SCAN_FOR_INFERENCE: usize = 10_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InferredKind {
    Date,
    DateTime,
    Float,
    String,
}

#[derive(Debug, Clone)]
pub struct ColumnTypeInference {
    found_date: Vec<bool>,
    found_datetime: Vec<bool>,
    found_float: Vec<bool>,
    found_string: Vec<bool>,
    found_non_null: Vec<bool>,
    rows_scanned: usize,
}

impl ColumnTypeInference {
    pub fn new(num_columns: usize) -> Self {
        Self {
            found_date: vec![false; num_columns],
            found_datetime: vec![false; num_columns],
            found_float: vec![false; num_columns],
            found_string: vec![false; num_columns],
            found_non_null: vec![false; num_columns],
            rows_scanned: 0,
        }
    }

    pub fn update<'a>(&mut self, row: impl IntoIterator<Item = &'a str>) {
        if self.rows_scanned >= MAX_ROWS_TO_SCAN_FOR_INFERENCE {
            return;
        }

        for (idx, value) in row.into_iter().enumerate() {
            if idx >= self.found_non_null.len() {
                break;
            }

            let trimmed = normalize_value(value);
            if trimmed.is_empty() {
                continue;
            }

            self.found_non_null[idx] = true;

            match infer_kind(trimmed) {
                InferredKind::String => self.found_string[idx] = true,
                InferredKind::Float => self.found_float[idx] = true,
                InferredKind::Date => self.found_date[idx] = true,
                InferredKind::DateTime => self.found_datetime[idx] = true,
            }
        }

        self.rows_scanned += 1;
    }

    pub fn inferred_clickhouse_types(&self) -> Vec<String> {
        (0..self.found_non_null.len())
            .map(|idx| {
                if self.found_string[idx] {
                    "String".to_string()
                } else if self.found_datetime[idx] {
                    "Nullable(DateTime64(3, 'UTC'))".to_string()
                } else if self.found_date[idx] {
                    "Nullable(Date)".to_string()
                } else if self.found_float[idx] {
                    "Nullable(Float64)".to_string()
                } else {
                    "Nullable(Float64)".to_string()
                }
            })
            .collect()
    }
}

fn infer_kind(value: &str) -> InferredKind {
    if value.parse::<f64>().is_ok() {
        return InferredKind::Float;
    }

    if looks_like_nem_datetime(value) && parse_nem_datetime(value).is_some() {
        return InferredKind::DateTime;
    }

    if looks_like_nem_date(value) && parse_nem_date(value).is_some() {
        return InferredKind::Date;
    }

    InferredKind::String
}

fn normalize_value(value: &str) -> &str {
    value.trim().trim_matches('"').trim()
}

#[cfg(test)]
mod tests {
    use super::ColumnTypeInference;

    #[test]
    fn infers_types_from_non_null_values() {
        let mut inference = ColumnTypeInference::new(4);
        inference.update(["2026-03-10 00:05:00", "2026-03-10", "12.5", "NSW1"]);

        assert_eq!(
            inference.inferred_clickhouse_types(),
            vec![
                "Nullable(DateTime64(3, 'UTC'))",
                "Nullable(Date)",
                "Nullable(Float64)",
                "String",
            ]
        );
    }

    #[test]
    fn defaults_all_null_columns_to_nullable_float() {
        let mut inference = ColumnTypeInference::new(2);
        inference.update(["", ""]);

        assert_eq!(
            inference.inferred_clickhouse_types(),
            vec!["Nullable(Float64)", "Nullable(Float64)"]
        );
    }
}
