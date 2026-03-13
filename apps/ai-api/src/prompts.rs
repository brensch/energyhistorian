use chrono::Utc;

use serde_json::Value;

use crate::models::{Plan, SemanticObject};

pub const REGION_ALIAS_GUIDANCE: &str = "Canonical NEM region IDs are NSW1, QLD1, VIC1, SA1, TAS1, and SNOWY1. Normalize natural-language region names before writing SQL. Do not use VIC, NSW, QLD, SA, or TAS in REGIONID filters.";

pub const PHYSICAL_FUEL_BLOCK_GUIDANCE: &str = "Do not answer questions about physical fuel consumed, gas used, coal burned, stockpiles, or inventories with dispatch or generation proxies. Block those unless the registry explicitly includes that physical dataset.";
pub const FUEL_MIX_SURFACE_GUIDANCE: &str = "For whole-of-market or region-wide generation breakdowns by fuel type, do not use semantic.actual_gen_duid because it has partial metered coverage and can badly undercount major fuels. Prefer semantic.daily_unit_dispatch joined to semantic.unit_dimension and use TOTALCLEARED * 0.5 as a dispatch-energy proxy, clearly noting that it is a dispatch proxy rather than metered generation. Use semantic.actual_gen_duid only for DUID-specific actual-output questions or questions explicitly about that metered surface.";

pub fn planner_system_prompt() -> String {
    format!(
        "You are a careful NEM analytics planner. You must decide whether the user's question can be answered from the provided semantic registry. If answerable, write one read-only ClickHouse SQL query using only semantic.* objects from the registry. If not answerable, return blocked with an empty SQL string. Prefer concise SQL with explicit grouping and limits. {REGION_ALIAS_GUIDANCE} {PHYSICAL_FUEL_BLOCK_GUIDANCE} {FUEL_MIX_SURFACE_GUIDANCE} Return JSON only."
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
        "You are repairing a failed ClickHouse SQL plan for Australia's National Electricity Market. Keep the same JSON schema as before. Only use semantic.* objects from the registry. {REGION_ALIAS_GUIDANCE} {PHYSICAL_FUEL_BLOCK_GUIDANCE} {FUEL_MIX_SURFACE_GUIDANCE} If the question cannot be answered cleanly, return blocked. Return JSON only."
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
    "You are a careful NEM analyst. Write a concise answer to the user's question based on the query result and a short analyst note. State the substantive answer first. Focus on key insights, comparisons, extremes, trends, or notable caveats. Do not narrate the chart. Do not restate the table schema, column list, row count, date coverage, or print raw rows unless the user explicitly asked for that. Do not say things like 'the chart shows', 'the table contains', or 'Total rows'. Avoid generic meta-commentary. Respect the provided caveats and confidence. Return JSON only with keys answer and note."
}

pub fn chart_system_prompt() -> &'static str {
    "You are a careful analytics visualization planner. Choose the most useful chart for the query result. You may return renderer=summary, renderer=table, or renderer=vega_lite. If you return vega_lite, return a valid Vega-Lite v6 spec without inline data.values because the frontend will inject the query rows. Use only fields present in the query result unless you explicitly create derived fields with transform.fold using existing columns. Prefer readable charts over clever ones: rankings usually want horizontal bars, time series usually want lines, composition over time can use stacked bars or areas, and dense wide outputs should fall back to a table. Avoid redundant encodings like coloring every unique bar by its own label or coloring by a field that is nearly constant. Prefer human-readable labels like station names over opaque IDs like DUID when both are available, and prefer small meaningful groupings like region for color when useful. Return JSON only."
}

pub fn chart_user_prompt(
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
            "chart_title": plan.chart_title,
            "used_objects": plan.used_objects,
        },
        "chart_requirements": {
            "allowed_renderers": ["summary", "table", "vega_lite"],
            "vega_lite_schema": "https://vega.github.io/schema/vega-lite/v6.json",
            "forbidden": [
                "inline data.values",
                "fields not present in the result unless created by transform.fold",
                "redundant color encodings on unique categories"
            ]
        },
        "results_summary": {
            "row_count": rows.len(),
            "columns": columns,
            "preview": rows.iter().take(20).collect::<Vec<_>>()
        },
        "required_output_examples": {
            "summary": {
                "renderer": "summary",
                "title": "string"
            },
            "table": {
                "renderer": "table",
                "title": "string"
            },
            "vega_lite": {
                "renderer": "vega_lite",
                "title": "string",
                "spec": {
                    "$schema": "https://vega.github.io/schema/vega-lite/v6.json"
                }
            }
        }
    });
    serde_json::to_string_pretty(&payload).expect("chart prompt")
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
