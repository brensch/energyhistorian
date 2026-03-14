use chrono::Utc;

use serde_json::Value;

use crate::models::{Plan, SemanticObject};

const RULES: &str = include_str!("../rules.md");

pub const REGION_ALIAS_GUIDANCE: &str = "Canonical NEM region IDs are NSW1, QLD1, VIC1, SA1, TAS1, and SNOWY1. Normalize natural-language region names before writing SQL. Do not use VIC, NSW, QLD, SA, or TAS in REGIONID filters.";

pub const PHYSICAL_FUEL_BLOCK_GUIDANCE: &str = "Do not answer questions about physical fuel consumed, gas used, coal burned, stockpiles, or inventories with dispatch or generation proxies. Block those unless the registry explicitly includes that physical dataset.";
pub const FUEL_MIX_SURFACE_GUIDANCE: &str = "For whole-of-market or region-wide generation breakdowns by fuel type, do not use semantic.actual_gen_duid because it has partial metered coverage and can badly undercount major fuels. Prefer semantic.daily_unit_dispatch joined to semantic.unit_dimension and use TOTALCLEARED * 0.5 as a dispatch-energy proxy, clearly noting that it is a dispatch proxy rather than metered generation. Use semantic.actual_gen_duid only for DUID-specific actual-output questions or questions explicitly about that metered surface.";

fn output_schema() -> serde_json::Value {
    serde_json::json!({
        "status": "answerable|blocked",
        "sql": "string",
        "used_objects": ["semantic.object_name"],
        "data_description": "string",
        "note": "string",
        "chart_title": "string",
        "chart_type": "line|bar|scatter|table|area|pie|box",
        "x": "string|null",
        "y": ["column_name"],
        "y2": ["column_name"],
        "color": "string|null",
        "y_label": "string|null",
        "y2_label": "string|null",
        "confidence": "high|medium|low",
        "reason": "string"
    })
}

pub fn planner_system_prompt() -> String {
    format!(
        "You are a careful NEM analytics planner. You must decide whether the user's question can be answered from the provided semantic registry. If answerable, write one read-only ClickHouse SQL query using only semantic.* objects from the registry and choose the chart configuration in the same response. If not answerable, return blocked with an empty SQL string. Prefer concise SQL with explicit grouping and limits. {REGION_ALIAS_GUIDANCE} {PHYSICAL_FUEL_BLOCK_GUIDANCE} {FUEL_MIX_SURFACE_GUIDANCE}\n\n{RULES}\n\nReturn JSON only."
    )
}

pub fn planner_user_prompt(
    question: &str,
    registry: &[SemanticObject],
    approved: Option<&str>,
    thread_context: &Value,
) -> String {
    let payload = serde_json::json!({
        "question": question,
        "approved_visualization_plan": approved,
        "thread_context": thread_context,
        "today": Utc::now().date_naive().to_string(),
        "required_output_schema": output_schema(),
        "semantic_registry": registry
    });
    serde_json::to_string_pretty(&payload).expect("planner prompt")
}

pub fn repair_system_prompt() -> String {
    format!(
        "You are repairing a failed ClickHouse SQL plan for Australia's National Electricity Market. Keep the same JSON schema as before. Only use semantic.* objects from the registry. {REGION_ALIAS_GUIDANCE} {PHYSICAL_FUEL_BLOCK_GUIDANCE} {FUEL_MIX_SURFACE_GUIDANCE}\n\n{RULES}\n\nIf the question cannot be answered cleanly, return blocked. Return JSON only."
    )
}

pub fn repair_user_prompt(
    question: &str,
    registry: &[SemanticObject],
    plan: &Plan,
    error: &str,
    thread_context: &Value,
) -> String {
    let payload = serde_json::json!({
        "question": question,
        "thread_context": thread_context,
        "failing_plan": plan,
        "error": error,
        "semantic_registry": registry,
        "required_output_schema": output_schema()
    });
    serde_json::to_string_pretty(&payload).expect("repair prompt")
}

pub fn answer_system_prompt() -> &'static str {
    "You are a careful NEM analyst. The query result is already the answer surface. Write a concise answer to the user's question based on the query result and a short analyst note. State the substantive answer first. Focus on key insights, comparisons, extremes, trends, or notable caveats. If the result is an approximation or proxy, say that plainly. Do not narrate the chart. Do not restate the table schema, column list, row count, date coverage, or print raw rows unless the user explicitly asked for that. Do not say things like 'the chart shows', 'the table contains', or 'Total rows'. Never provide code, pseudocode, Python, matplotlib, pandas, Plotly JSON, or instructions for how to produce a chart. The application already renders the chart. Avoid generic meta-commentary. Respect the provided caveats and confidence. Return JSON only with keys answer and note."
}

pub fn answer_repair_system_prompt() -> &'static str {
    "You are repairing an analytics answer that violated product rules. Return a concise direct answer to the question, not instructions. Never include code, code fences, pseudocode, Python, matplotlib, pandas, Plotly JSON, or implementation steps. Never narrate the chart or describe table structure. The chart is already rendered by the product. Return JSON only with keys answer and note."
}

pub fn answer_user_prompt(
    question: &str,
    plan: &Plan,
    columns: &[String],
    rows: &[Vec<serde_json::Value>],
    thread_context: &Value,
) -> String {
    let payload = serde_json::json!({
        "question": question,
        "thread_context": thread_context,
        "plan": {
            "data_description": plan.data_description,
            "note": plan.note,
            "confidence": plan.confidence,
            "reason": plan.reason,
            "used_objects": plan.used_objects
        },
        "style_guidance": {
            "primary_goal": "answer the question with analyst insight, not a mechanical restatement of the result set",
            "avoid": [
                "describing the chart",
                "describing the table layout",
                "listing columns",
                "mentioning row_count unless the user asked",
                "copying raw rows into the answer",
                "phrases like 'the chart shows', 'the table contains', or 'total rows'"
            ]
        },
        "results_summary": {
            "row_count": rows.len(),
            "columns": columns,
            "preview": rows.iter().take(12).collect::<Vec<_>>()
        },
        "required_output_schema": {
            "answer": "string",
            "note": "string"
        }
    });
    serde_json::to_string_pretty(&payload).expect("answer prompt")
}

pub fn answer_repair_user_prompt(
    question: &str,
    plan: &Plan,
    columns: &[String],
    rows: &[Vec<serde_json::Value>],
    thread_context: &Value,
    invalid_answer: &str,
    violation: &str,
) -> String {
    let payload = serde_json::json!({
        "question": question,
        "thread_context": thread_context,
        "violation": violation,
        "invalid_answer": invalid_answer,
        "plan": {
            "data_description": plan.data_description,
            "note": plan.note,
            "confidence": plan.confidence,
            "reason": plan.reason,
            "used_objects": plan.used_objects
        },
        "style_guidance": {
            "primary_goal": "answer the question with a short analyst takeaway, not instructions or chart narration",
            "must_not_do": [
                "include any code",
                "mention Python, pandas, matplotlib, Plotly, or snippets",
                "describe how to make a chart",
                "describe the table layout or raw rows",
                "say phrases like 'the chart shows', 'the table contains', or 'total rows'"
            ]
        },
        "results_summary": {
            "row_count": rows.len(),
            "columns": columns,
            "preview": rows.iter().take(12).collect::<Vec<_>>()
        },
        "required_output_schema": {
            "answer": "string",
            "note": "string"
        }
    });
    serde_json::to_string_pretty(&payload).expect("answer repair prompt")
}
