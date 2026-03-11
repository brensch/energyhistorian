use std::collections::BTreeMap;
use std::fs;

use anyhow::{Context, Result};
use ingest_core::{ArtifactKind, LocalArtifact, RawPluginParseResult, RawPluginTableBatch};
use scraper::{Html, Selector};
use serde_json::{Map, Value, json};

use crate::discovery::DATA_MODEL_INDEX_URL;
use crate::mms_parser;

pub fn parse_artifact(
    collection_id: &str,
    artifact: &LocalArtifact,
) -> Result<RawPluginParseResult> {
    let mut tables = vec![RawPluginTableBatch {
        table_name: "document_artifacts".to_string(),
        rows: vec![json!({
            "document_type": document_type(artifact),
            "artifact_kind": format!("{:?}", artifact.metadata.kind),
            "local_path": artifact.local_path.display().to_string(),
            "acquisition_uri": artifact.metadata.acquisition_uri,
            "model_version": artifact.metadata.model_version,
            "release_name": artifact.metadata.release_name,
            "fetched_at": artifact.metadata.fetched_at.map(|ts| ts.to_rfc3339()),
            "content_sha256": artifact.metadata.content_sha256,
            "content_length_bytes": artifact.metadata.content_length_bytes,
        })],
    }];

    if artifact.local_path.is_dir() {
        let (table_rows, column_rows) = parse_model_bundle(artifact)?;
        if !table_rows.is_empty() {
            tables.push(RawPluginTableBatch {
                table_name: "table_explanations".to_string(),
                rows: table_rows,
            });
        }
        if !column_rows.is_empty() {
            tables.push(RawPluginTableBatch {
                table_name: "column_explanations".to_string(),
                rows: column_rows,
            });
        }
        return Ok(RawPluginParseResult { tables });
    }

    let html = fs::read_to_string(&artifact.local_path)
        .with_context(|| format!("reading {}", artifact.local_path.display()))?;

    match collection_id {
        "current-data-model" => {
            if artifact.metadata.acquisition_uri == DATA_MODEL_INDEX_URL {
                let refs = parse_document_references(&html);
                if !refs.is_empty() {
                    tables.push(RawPluginTableBatch {
                        table_name: "document_references".to_string(),
                        rows: refs,
                    });
                }
            }
        }
        "report-relationships" => {
            let rows = parse_html_table_rows(&html);
            if !rows.is_empty() {
                tables.push(RawPluginTableBatch {
                    table_name: "report_mappings".to_string(),
                    rows,
                });
            }
        }
        "population-dates" => {
            let rows = parse_html_table_rows(&html);
            if !rows.is_empty() {
                tables.push(RawPluginTableBatch {
                    table_name: "population_dates".to_string(),
                    rows,
                });
            }
        }
        _ => {}
    }

    Ok(RawPluginParseResult { tables })
}

fn parse_model_bundle(artifact: &LocalArtifact) -> Result<(Vec<Value>, Vec<Value>)> {
    let toc_html = fs::read_to_string(artifact.local_path.join("_toc.htm"))
        .context("reading bundled metadata TOC")?;
    let toc_entries = mms_parser::parse_toc(&toc_html)?;
    let (_packages, tables) =
        mms_parser::parse_model_from_files(&artifact.local_path, &toc_entries)?;

    let mut table_rows = Vec::new();
    let mut column_rows = Vec::new();
    let model_version = artifact.metadata.model_version.clone();
    let release_name = artifact.metadata.release_name.clone();

    for table in tables {
        table_rows.push(json!({
            "model_version": model_version,
            "release_name": release_name,
            "package_id": table.package_id,
            "table_name": table.table_name,
            "description": table.description,
            "source_notes": table.source_notes,
            "volume_notes": table.volume_notes,
            "visibility": table.visibility,
            "additional_notes": table.additional_notes,
            "primary_key_columns": serde_json::to_string(&table.primary_key_columns).unwrap_or_default(),
            "index_columns": serde_json::to_string(&table.index_columns).unwrap_or_default(),
        }));

        for column in table.columns {
            column_rows.push(json!({
                "model_version": model_version,
                "release_name": release_name,
                "package_id": table.package_id,
                "table_name": table.table_name,
                "column_name": column.column_name,
                "ordinal": column.ordinal,
                "oracle_type": column.oracle_type,
                "is_mandatory": column.is_mandatory,
                "is_primary_key": column.is_primary_key,
                "description": column.description,
                "units": column.units,
                "valid_values": column.valid_values,
            }));
        }
    }

    Ok((table_rows, column_rows))
}

fn parse_document_references(html: &str) -> Vec<Value> {
    let document = Html::parse_document(html);
    let selector = Selector::parse(r#"a[href$=".pdf"]"#).unwrap();

    document
        .select(&selector)
        .map(|anchor| {
            let title = normalize_text(&anchor.text().collect::<String>());
            let href = anchor.value().attr("href").unwrap_or_default();
            json!({
                "title": title,
                "href": href,
                "document_family": document_family(&title),
                "model_version": extract_version_from_text(&title).or_else(|| extract_version_from_text(href)),
            })
        })
        .collect()
}

fn parse_html_table_rows(html: &str) -> Vec<Value> {
    let document = Html::parse_document(html);
    let table_sel = Selector::parse("table").unwrap();
    let row_sel = Selector::parse("tr").unwrap();
    let cell_sel = Selector::parse("th, td").unwrap();
    let mut rows = Vec::new();

    for table in document.select(&table_sel) {
        let mut headers: Vec<String> = Vec::new();
        for (row_idx, row) in table.select(&row_sel).enumerate() {
            let cells: Vec<String> = row
                .select(&cell_sel)
                .map(|cell| normalize_text(&cell.text().collect::<String>()))
                .filter(|value| !value.is_empty())
                .collect();
            if cells.is_empty() {
                continue;
            }
            if row_idx == 0 {
                headers = cells
                    .iter()
                    .enumerate()
                    .map(|(idx, cell)| {
                        if cell.is_empty() {
                            format!("column_{idx}")
                        } else {
                            slug(cell)
                        }
                    })
                    .collect();
                continue;
            }

            let payload = row_payload(&headers, &cells);
            if payload.is_empty() {
                continue;
            }
            rows.push(Value::Object(
                payload.into_iter().collect::<Map<String, Value>>(),
            ));
        }
    }

    rows
}

fn row_payload(headers: &[String], cells: &[String]) -> BTreeMap<String, Value> {
    headers
        .iter()
        .enumerate()
        .filter_map(|(idx, header)| {
            let value = cells.get(idx).cloned().unwrap_or_default();
            if header.is_empty() && value.is_empty() {
                None
            } else {
                Some((header.clone(), Value::String(value)))
            }
        })
        .collect()
}

fn document_family(title: &str) -> &'static str {
    let lower = title.to_ascii_lowercase();
    if lower.contains("upgrade") {
        "upgrade_report"
    } else if lower.contains("package summary") {
        "package_summary"
    } else if lower.contains("data model report") {
        "data_model_report"
    } else {
        "other"
    }
}

fn document_type(artifact: &LocalArtifact) -> &'static str {
    match artifact.metadata.kind {
        ArtifactKind::HtmlDocument => "html",
        ArtifactKind::PdfDocument => "pdf",
        ArtifactKind::CsvExport | ArtifactKind::CsvFile => "csv",
        _ => "binary",
    }
}

fn extract_version_from_text(text: &str) -> Option<String> {
    text.chars()
        .collect::<Vec<_>>()
        .windows(3)
        .find_map(|window| match window {
            [left, '.', right] if left.is_ascii_digit() && right.is_ascii_digit() => {
                Some(format!("{left}.{right}"))
            }
            _ => None,
        })
}

fn normalize_text(input: &str) -> String {
    input.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn slug(input: &str) -> String {
    input
        .to_ascii_lowercase()
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect()
}
