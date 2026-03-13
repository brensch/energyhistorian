use chrono::Utc;

use crate::models::{Plan, SemanticObject};

pub const REGION_ALIAS_GUIDANCE: &str = "Canonical NEM region IDs are NSW1, QLD1, VIC1, SA1, TAS1, and SNOWY1. Normalize natural-language region names before writing SQL. Do not use VIC, NSW, QLD, SA, or TAS in REGIONID filters.";

pub const PHYSICAL_FUEL_BLOCK_GUIDANCE: &str = "Do not answer questions about physical fuel consumed, gas used, coal burned, stockpiles, or inventories with dispatch or generation proxies. Block those unless the registry explicitly includes that physical dataset.";

pub fn planner_system_prompt() -> String {
    format!(
        "You are a careful NEM analytics planner. You must decide whether the user's question can be answered from the provided semantic registry. If answerable, write one read-only ClickHouse SQL query using only semantic.* objects from the registry. If not answerable, return blocked with an empty SQL string. Prefer concise SQL with explicit grouping and limits. {REGION_ALIAS_GUIDANCE} {PHYSICAL_FUEL_BLOCK_GUIDANCE} Return JSON only."
    )
}

pub fn planner_user_prompt(
    question: &str,
    registry: &[SemanticObject],
    approved: Option<&str>,
) -> String {
    let payload = serde_json::json!({
        "question": question,
        "approved_visualization_plan": approved,
        "today": Utc::now().date_naive().to_string(),
        "required_output_schema": {
            "status": "answerable|blocked",
            "sql": "string",
            "used_objects": ["semantic.object_name"],
            "data_description": "string",
            "note": "string",
            "chart_title": "string",
            "confidence": "high|medium|low",
            "reason": "string"
        },
        "semantic_registry": registry
    });
    serde_json::to_string_pretty(&payload).expect("planner prompt")
}

pub fn repair_system_prompt() -> String {
    format!(
        "You are repairing a failed ClickHouse SQL plan for Australia's National Electricity Market. Keep the same JSON schema as before. Only use semantic.* objects from the registry. {REGION_ALIAS_GUIDANCE} {PHYSICAL_FUEL_BLOCK_GUIDANCE} If the question cannot be answered cleanly, return blocked. Return JSON only."
    )
}

pub fn repair_user_prompt(
    question: &str,
    registry: &[SemanticObject],
    plan: &Plan,
    error: &str,
) -> String {
    let payload = serde_json::json!({
        "question": question,
        "failing_plan": plan,
        "error": error,
        "semantic_registry": registry,
        "required_output_schema": {
            "status": "answerable|blocked",
            "sql": "string",
            "used_objects": ["semantic.object_name"],
            "data_description": "string",
            "note": "string",
            "chart_title": "string",
            "confidence": "high|medium|low",
            "reason": "string"
        }
    });
    serde_json::to_string_pretty(&payload).expect("repair prompt")
}

pub fn answer_system_prompt() -> &'static str {
    "You are a careful NEM analyst. Write a concise answer to the user's question based on the query result and a short analyst note. State the substantive answer first. Avoid generic meta-commentary. Respect the provided caveats and confidence. Return JSON only with keys answer and note."
}

pub fn answer_user_prompt(
    question: &str,
    plan: &Plan,
    columns: &[String],
    rows: &[Vec<serde_json::Value>],
) -> String {
    let payload = serde_json::json!({
        "question": question,
        "plan": {
            "data_description": plan.data_description,
            "note": plan.note,
            "confidence": plan.confidence,
            "reason": plan.reason,
            "used_objects": plan.used_objects
        },
        "results_summary": {
            "row_count": rows.len(),
            "columns": columns,
            "preview": rows.iter().take(20).collect::<Vec<_>>()
        },
        "required_output_schema": {
            "answer": "string",
            "note": "string"
        }
    });
    serde_json::to_string_pretty(&payload).expect("answer prompt")
}
