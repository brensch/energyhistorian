use std::{collections::HashSet, time::Instant};

use anyhow::{Result, bail};
use regex::Regex;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::{
    db::Store,
    llm::{CompletionResponse, estimate_cost_usd, extract_json},
    models::{
        AuthUser, ChartSpec, ChatRequest, FinalAnswer, Plan, QueryPreview, SemanticObject,
        StreamPayload,
    },
    prompts,
    state::AppState,
};

const MAX_ATTEMPTS: usize = 3;

pub async fn execute_chat(
    state: AppState,
    user: AuthUser,
    request: ChatRequest,
    sender: mpsc::Sender<StreamPayload>,
) -> Result<()> {
    state.store.sync_user(&user).await?;

    let conversation = state
        .store
        .ensure_conversation(
            &user.org_id,
            &user.id,
            request.conversation_id,
            &title_from_question(&request.question),
        )
        .await?;
    let run_id = state
        .store
        .create_run(conversation.id, &user.org_id, &user.id, &request.question)
        .await?;

    emit(
        &sender,
        StreamPayload::RunStarted {
            run_id,
            conversation_id: conversation.id,
        },
    )
    .await;

    state
        .store
        .append_message(
            conversation.id,
            &user.org_id,
            &user.id,
            Some(run_id),
            "user",
            &request.question,
            None,
            json!({}),
        )
        .await?;

    emit_status(&sender, "registry", "Loading semantic registry").await;
    let registry = state.clickhouse.load_registry().await?;
    if registry.is_empty() {
        bail!("semantic registry is empty");
    }

    emit_status(&sender, "planning", "Planning query").await;
    let mut plan = enforce_semantic_contract(
        &request.question,
        plan_question(
            &state,
            &user,
            &request.question,
            &registry,
            request.approved_proposal.as_deref(),
            run_id,
        )
        .await?,
    );

    emit(&sender, StreamPayload::Plan { plan: plan.clone() }).await;

    if plan.status != "answerable" || plan.sql.trim().is_empty() {
        let reason = if plan.reason.is_empty() {
            "Question could not be answered from the current semantic layer.".to_string()
        } else {
            plan.reason.clone()
        };
        let answer = FinalAnswer {
            answer: reason.clone(),
            note: plan.note.clone(),
        };
        state
            .store
            .append_message(
                conversation.id,
                &user.org_id,
                &user.id,
                Some(run_id),
                "assistant",
                &answer.answer,
                None,
                Store::assistant_metadata(
                    None,
                    &plan.used_objects,
                    None,
                    None,
                    Some(&answer.note),
                    Some(&plan.confidence),
                ),
            )
            .await?;
        state
            .store
            .finish_run(run_id, "completed", None, None)
            .await?;
        emit(&sender, StreamPayload::Answer { answer }).await;
        emit(&sender, StreamPayload::Completed { run_id }).await;
        return Ok(());
    }

    let allowed_objects = registry
        .iter()
        .map(|item| item.object_name.clone())
        .collect::<HashSet<_>>();

    let mut preview = None;
    let mut last_error = None;
    for attempt in 0..MAX_ATTEMPTS {
        emit(
            &sender,
            StreamPayload::Sql {
                sql: plan.sql.clone(),
            },
        )
        .await;
        match run_validated_query(&state, &user, run_id, &plan.sql, &allowed_objects).await {
            Ok(result) if result.row_count > 0 => {
                preview = Some(result);
                last_error = None;
                break;
            }
            Ok(_) => {
                last_error = Some("Query returned no rows".to_string());
            }
            Err(error) => {
                last_error = Some(error.to_string());
            }
        }

        if attempt + 1 >= MAX_ATTEMPTS {
            break;
        }
        emit_status(&sender, "repair", "Repairing SQL plan").await;
        plan = enforce_semantic_contract(
            &request.question,
            repair_plan(
                &state,
                &user,
                &request.question,
                &registry,
                &plan,
                last_error.as_deref().unwrap_or("unknown error"),
                run_id,
            )
            .await?,
        );
        emit(&sender, StreamPayload::Plan { plan: plan.clone() }).await;
        if plan.status != "answerable" || plan.sql.trim().is_empty() {
            break;
        }
    }

    let Some(preview) = preview else {
        let message = last_error.unwrap_or_else(|| "Query could not be executed".to_string());
        state
            .store
            .finish_run(run_id, "failed", Some(&plan.sql), Some(&message))
            .await?;
        emit(
            &sender,
            StreamPayload::Failed {
                message: message.clone(),
            },
        )
        .await;
        return Ok(());
    };

    let chart = build_chart_spec(&preview, &plan.chart_title);
    emit(
        &sender,
        StreamPayload::QueryPreview {
            preview: preview.clone(),
            chart: chart.clone(),
        },
    )
    .await;

    emit_status(&sender, "answer", "Writing answer").await;
    let answer = refine_answer(&state, &user, &request.question, &plan, &preview, run_id).await?;
    state
        .store
        .append_message(
            conversation.id,
            &user.org_id,
            &user.id,
            Some(run_id),
            "assistant",
            &answer.answer,
            Some(&plan.sql),
            Store::assistant_metadata(
                Some(&plan.sql),
                &plan.used_objects,
                Some(&chart),
                Some(&preview),
                Some(&answer.note),
                Some(&plan.confidence),
            ),
        )
        .await?;
    state
        .store
        .finish_run(run_id, "completed", Some(&plan.sql), None)
        .await?;
    emit(&sender, StreamPayload::Answer { answer }).await;
    emit(&sender, StreamPayload::Completed { run_id }).await;
    Ok(())
}

async fn plan_question(
    state: &AppState,
    user: &AuthUser,
    question: &str,
    registry: &[SemanticObject],
    approved_proposal: Option<&str>,
    run_id: Uuid,
) -> Result<Plan> {
    let response = state
        .llm
        .json_completion(
            &prompts::planner_system_prompt(),
            &prompts::planner_user_prompt(question, registry, approved_proposal),
        )
        .await?;
    log_llm_usage(state, user, run_id, "planner", &response).await?;
    Ok(extract_json::<Plan>(&response.text)?)
}

async fn repair_plan(
    state: &AppState,
    user: &AuthUser,
    question: &str,
    registry: &[SemanticObject],
    plan: &Plan,
    error: &str,
    run_id: Uuid,
) -> Result<Plan> {
    let response = state
        .llm
        .json_completion(
            &prompts::repair_system_prompt(),
            &prompts::repair_user_prompt(question, registry, plan, error),
        )
        .await?;
    log_llm_usage(state, user, run_id, "repair", &response).await?;
    Ok(extract_json::<Plan>(&response.text)?)
}

async fn refine_answer(
    state: &AppState,
    user: &AuthUser,
    question: &str,
    plan: &Plan,
    preview: &QueryPreview,
    run_id: Uuid,
) -> Result<FinalAnswer> {
    let response = state
        .llm
        .json_completion(
            prompts::answer_system_prompt(),
            &prompts::answer_user_prompt(question, plan, &preview.columns, &preview.rows),
        )
        .await?;
    log_llm_usage(state, user, run_id, "answer", &response).await?;
    Ok(extract_json::<FinalAnswer>(&response.text)?)
}

async fn log_llm_usage(
    state: &AppState,
    user: &AuthUser,
    run_id: Uuid,
    prompt_type: &str,
    response: &CompletionResponse,
) -> Result<()> {
    let cost = estimate_cost_usd(
        state.config.llm_provider,
        &response.model,
        response.input_tokens,
        response.output_tokens,
    );
    let request_id = Uuid::new_v4().to_string();
    state
        .clickhouse
        .log_llm_usage(
            &request_id,
            &user.id,
            &user.email,
            &user.org_id,
            &user.session_id,
            &response.model,
            prompt_type,
            response.input_tokens,
            response.output_tokens,
            cost,
        )
        .await?;
    state
        .store
        .record_usage(
            &user.org_id,
            &user.id,
            Some(run_id),
            "llm_request",
            1,
            "request",
            json!({ "model": response.model, "prompt_type": prompt_type }),
        )
        .await?;
    state
        .store
        .record_usage(
            &user.org_id,
            &user.id,
            Some(run_id),
            "llm_cost_usd_micros",
            (cost * 1_000_000.0).round() as i64,
            "usd_micros",
            json!({ "model": response.model, "prompt_type": prompt_type }),
        )
        .await?;
    Ok(())
}

async fn run_validated_query(
    state: &AppState,
    user: &AuthUser,
    run_id: Uuid,
    sql: &str,
    allowed_objects: &HashSet<String>,
) -> Result<QueryPreview> {
    let sanitized = validate_sql(sql, allowed_objects)?;
    state.clickhouse.explain_syntax(&sanitized).await?;
    let started = Instant::now();
    let preview_result = state.clickhouse.query_preview(&sanitized).await;
    let duration_ms = started.elapsed().as_millis();

    match &preview_result {
        Ok(preview) => {
            state
                .clickhouse
                .log_query_usage(
                    &Uuid::new_v4().to_string(),
                    &user.id,
                    &user.email,
                    &user.org_id,
                    &user.session_id,
                    &sanitized,
                    preview.row_count,
                    duration_ms,
                    true,
                    None,
                )
                .await
                .ok();
            state
                .store
                .record_usage(
                    &user.org_id,
                    &user.id,
                    Some(run_id),
                    "clickhouse_query",
                    1,
                    "query",
                    json!({ "rows": preview.row_count, "duration_ms": duration_ms }),
                )
                .await?;
        }
        Err(error) => {
            state
                .clickhouse
                .log_query_usage(
                    &Uuid::new_v4().to_string(),
                    &user.id,
                    &user.email,
                    &user.org_id,
                    &user.session_id,
                    &sanitized,
                    0,
                    duration_ms,
                    false,
                    Some(&error.to_string()),
                )
                .await
                .ok();
        }
    }

    preview_result
}

fn validate_sql(sql: &str, allowed_objects: &HashSet<String>) -> Result<String> {
    let sql = sql.trim().trim_end_matches(';').to_string();
    let lowered = sql.to_ascii_lowercase();
    if !(lowered.starts_with("select") || lowered.starts_with("with")) {
        bail!("only SELECT/WITH queries are allowed");
    }
    let forbidden = [
        "insert", "delete", "update", "drop", "truncate", "alter", "create", "attach", "detach",
        "grant", "revoke",
    ];
    if forbidden.iter().any(|keyword| {
        Regex::new(&format!(r"\b{keyword}\b"))
            .expect("regex")
            .is_match(&lowered)
    }) {
        bail!("query contains forbidden write or DDL keyword");
    }
    if lowered.contains("system.")
        || Regex::new(r"\braw_[a-z0-9_]+\b")
            .expect("regex")
            .is_match(&lowered)
    {
        bail!("query must stay within semantic.* objects");
    }

    let ref_regex = Regex::new(r"semantic\.[A-Za-z0-9_]+").expect("regex");
    let referenced = ref_regex
        .find_iter(&sql)
        .map(|item| item.as_str().to_string())
        .collect::<HashSet<_>>();
    if referenced.is_empty() {
        bail!("query did not reference any semantic objects");
    }
    let unknown = referenced
        .difference(allowed_objects)
        .cloned()
        .collect::<Vec<_>>();
    if !unknown.is_empty() {
        bail!(
            "query referenced objects not in registry: {}",
            unknown.join(", ")
        );
    }
    Ok(sql)
}

fn enforce_semantic_contract(question: &str, plan: Plan) -> Plan {
    if plan.status == "answerable"
        && question_requests_physical_consumption(question)
        && plan.used_objects.iter().any(|item| {
            matches!(
                item.as_str(),
                "semantic.daily_unit_dispatch"
                    | "semantic.dispatch_unit_solution"
                    | "semantic.actual_gen_duid"
            )
        })
    {
        return Plan {
            status: "blocked".to_string(),
            sql: String::new(),
            used_objects: plan.used_objects,
            data_description: "The current semantic layer exposes dispatch, generation, bids, prices, and registration metadata, but not physical fuel-consumption or inventory measurements.".to_string(),
            note: "Blocked because the question asks about physical fuel use or inventory, and answering from dispatch or generation proxies would be misleading.".to_string(),
            chart_title: plan.chart_title,
            confidence: "low".to_string(),
            reason: "No physical fuel-consumption or inventory dataset exists in the current semantic surface.".to_string(),
        };
    }
    plan
}

fn question_requests_physical_consumption(question: &str) -> bool {
    let lowered = question.to_ascii_lowercase();
    if lowered.contains("battery") && (lowered.contains("charge") || lowered.contains("charging")) {
        return false;
    }
    [
        " used",
        "consumed",
        "consumption",
        "burned",
        "burnt",
        "stockpile",
        "inventory",
        "fuel use",
        "fuel consumed",
        "gas used",
        "coal used",
    ]
    .iter()
    .any(|pattern| lowered.contains(pattern))
}

fn build_chart_spec(preview: &QueryPreview, title: &str) -> ChartSpec {
    if preview.row_count <= 1 {
        return ChartSpec {
            kind: "summary".to_string(),
            x: None,
            y: Vec::new(),
            color: None,
            title: title.to_string(),
        };
    }
    let datetime_idx = preview.columns.iter().position(|column| {
        is_timeish_name(column) || column_values_look_like_datetimes(preview, column)
    });
    let numeric = preview
        .columns
        .iter()
        .filter(|column| column_values_look_numeric(preview, column))
        .cloned()
        .collect::<Vec<_>>();
    let categorical = preview
        .columns
        .iter()
        .filter(|column| {
            !numeric.contains(*column)
                && datetime_idx
                    .map(|idx| preview.columns[idx] != **column)
                    .unwrap_or(true)
        })
        .cloned()
        .collect::<Vec<_>>();

    if let Some(idx) = datetime_idx {
        return ChartSpec {
            kind: "line".to_string(),
            x: Some(preview.columns[idx].clone()),
            y: numeric.into_iter().take(2).collect(),
            color: categorical.first().cloned(),
            title: title.to_string(),
        };
    }
    if !categorical.is_empty() && !numeric.is_empty() {
        return ChartSpec {
            kind: "bar".to_string(),
            x: categorical.first().cloned(),
            y: vec![numeric[0].clone()],
            color: categorical.get(1).cloned(),
            title: title.to_string(),
        };
    }
    ChartSpec {
        kind: "table".to_string(),
        x: None,
        y: Vec::new(),
        color: None,
        title: title.to_string(),
    }
}

fn is_timeish_name(column: &str) -> bool {
    let upper = column.to_ascii_uppercase();
    upper.contains("DATE") || upper.contains("TIME") || upper.contains("INTERVAL")
}

fn column_values_look_numeric(preview: &QueryPreview, column: &str) -> bool {
    let Some(idx) = preview.columns.iter().position(|item| item == column) else {
        return false;
    };
    let mut seen = 0;
    for row in preview.rows.iter().take(20) {
        if let Some(value) = row.get(idx) {
            match value {
                Value::Number(_) => {
                    seen += 1;
                }
                Value::String(text) if text.parse::<f64>().is_ok() => {
                    seen += 1;
                }
                Value::Null => {}
                _ => return false,
            }
        }
    }
    seen > 0
}

fn column_values_look_like_datetimes(preview: &QueryPreview, column: &str) -> bool {
    let Some(idx) = preview.columns.iter().position(|item| item == column) else {
        return false;
    };
    let mut seen = 0;
    for row in preview.rows.iter().take(10) {
        if let Some(Value::String(text)) = row.get(idx) {
            if chrono::DateTime::parse_from_rfc3339(text).is_ok()
                || chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S").is_ok()
                || chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f").is_ok()
                || chrono::NaiveDate::parse_from_str(text, "%Y-%m-%d").is_ok()
            {
                seen += 1;
            }
        }
    }
    seen > 0
}

fn title_from_question(question: &str) -> String {
    let mut title = question.trim().replace('\n', " ");
    if title.len() > 80 {
        title.truncate(77);
        title.push_str("...");
    }
    title
}

async fn emit(sender: &mpsc::Sender<StreamPayload>, payload: StreamPayload) {
    let _ = sender.send(payload).await;
}

async fn emit_status(sender: &mpsc::Sender<StreamPayload>, phase: &str, message: &str) {
    emit(
        sender,
        StreamPayload::Status {
            phase: phase.to_string(),
            message: message.to_string(),
        },
    )
    .await;
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use serde_json::json;

    use super::{Plan, QueryPreview, build_chart_spec, enforce_semantic_contract, validate_sql};

    #[test]
    fn validate_sql_allows_semantic_select() {
        let allowed = HashSet::from(["semantic.dispatch_price".to_string()]);
        let sql = "SELECT REGIONID, avg(RRP) FROM semantic.dispatch_price GROUP BY REGIONID";
        let validated = validate_sql(sql, &allowed).expect("validated");
        assert!(validated.starts_with("SELECT"));
    }

    #[test]
    fn validate_sql_blocks_write_keywords() {
        let allowed = HashSet::from(["semantic.dispatch_price".to_string()]);
        let error = validate_sql(
            "SELECT * FROM semantic.dispatch_price; DROP TABLE semantic.dispatch_price",
            &allowed,
        )
        .expect_err("should fail");
        assert!(error.to_string().contains("forbidden"));
    }

    #[test]
    fn semantic_contract_blocks_fuel_proxy() {
        let blocked = enforce_semantic_contract(
            "How much gas was used yesterday?",
            Plan {
                status: "answerable".to_string(),
                sql: "SELECT * FROM semantic.daily_unit_dispatch".to_string(),
                used_objects: vec!["semantic.daily_unit_dispatch".to_string()],
                data_description: String::new(),
                note: String::new(),
                chart_title: "Gas".to_string(),
                confidence: "medium".to_string(),
                reason: String::new(),
            },
        );
        assert_eq!(blocked.status, "blocked");
        assert!(blocked.reason.contains("fuel-consumption"));
    }

    #[test]
    fn single_row_chart_uses_summary() {
        let preview = QueryPreview {
            columns: vec!["REGIONID".to_string(), "RRP".to_string()],
            rows: vec![vec![json!("NSW1"), json!(52.4)]],
            row_count: 1,
        };
        let chart = build_chart_spec(&preview, "Price");
        assert_eq!(chart.kind, "summary");
    }
}
