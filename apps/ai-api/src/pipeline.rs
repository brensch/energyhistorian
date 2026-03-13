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
        StreamPayload, ThreadContextMessage,
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
    let prompt_context = compact_thread_context(&request.thread_context);

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

    if let Some((plan, answer, chart, preview)) =
        maybe_build_visualization_follow_up(&request.question, &request.thread_context)
    {
        emit_status(
            &sender,
            "chart",
            "Reusing the previous result from this thread",
        )
        .await;
        emit(&sender, StreamPayload::Plan { plan: plan.clone() }).await;
        emit(
            &sender,
            StreamPayload::QueryPreview {
                preview: preview.clone(),
                chart: chart.clone(),
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
                "assistant",
                &answer.answer,
                None,
                Store::assistant_metadata(
                    None,
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
            .finish_run(run_id, "completed", None, None)
            .await?;
        emit(&sender, StreamPayload::Answer { answer }).await;
        emit(&sender, StreamPayload::Completed { run_id }).await;
        return Ok(());
    }

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
            &prompt_context,
            run_id,
        )
        .await?,
    );
    plan = maybe_redirect_partial_actual_generation_plan(
        &state,
        &user,
        &request.question,
        &registry,
        plan,
        run_id,
    )
    .await?;

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
                &prompt_context,
                run_id,
            )
            .await?,
        );
        plan = maybe_redirect_partial_actual_generation_plan(
            &state,
            &user,
            &request.question,
            &registry,
            plan,
            run_id,
        )
        .await?;
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

    emit_status(&sender, "chart", "Shaping chart").await;
    let chart = match plan_chart(
        &state,
        &user,
        &request.question,
        &plan,
        &preview,
        &prompt_context,
        run_id,
    )
    .await
    {
        Ok(chart) if validate_chart_spec(&chart, &preview).is_ok() => chart,
        _ => build_chart_spec(&preview, &plan.chart_title),
    };
    emit(
        &sender,
        StreamPayload::QueryPreview {
            preview: preview.clone(),
            chart: chart.clone(),
        },
    )
    .await;

    emit_status(&sender, "answer", "Writing answer").await;
    let answer = refine_answer(
        &state,
        &user,
        &request.question,
        &plan,
        &preview,
        &prompt_context,
        run_id,
    )
    .await?;
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
    thread_context: &Value,
    run_id: Uuid,
) -> Result<Plan> {
    let response = state
        .llm
        .json_completion(
            &prompts::planner_system_prompt(),
            &prompts::planner_user_prompt(question, registry, approved_proposal, thread_context),
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
    thread_context: &Value,
    run_id: Uuid,
) -> Result<Plan> {
    let response = state
        .llm
        .json_completion(
            &prompts::repair_system_prompt(),
            &prompts::repair_user_prompt(question, registry, plan, error, thread_context),
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
    thread_context: &Value,
    run_id: Uuid,
) -> Result<FinalAnswer> {
    let response = state
        .llm
        .json_completion(
            prompts::answer_system_prompt(),
            &prompts::answer_user_prompt(
                question,
                plan,
                &preview.columns,
                &preview.rows,
                thread_context,
            ),
        )
        .await?;
    log_llm_usage(state, user, run_id, "answer", &response).await?;
    Ok(extract_json::<FinalAnswer>(&response.text)?)
}

async fn plan_chart(
    state: &AppState,
    user: &AuthUser,
    question: &str,
    plan: &Plan,
    preview: &QueryPreview,
    thread_context: &Value,
    run_id: Uuid,
) -> Result<ChartSpec> {
    let response = state
        .llm
        .json_completion(
            prompts::chart_system_prompt(),
            &prompts::chart_user_prompt(
                question,
                plan,
                &preview.columns,
                &preview.rows,
                thread_context,
            ),
        )
        .await?;
    log_llm_usage(state, user, run_id, "chart", &response).await?;
    Ok(extract_json::<ChartSpec>(&response.text)?)
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

async fn maybe_redirect_partial_actual_generation_plan(
    state: &AppState,
    user: &AuthUser,
    question: &str,
    registry: &[SemanticObject],
    plan: Plan,
    run_id: Uuid,
) -> Result<Plan> {
    if !question_requests_generation_mix(question) || !plan_uses_actual_gen_duid(&plan) {
        return Ok(plan);
    }

    let repaired = enforce_semantic_contract(
        question,
        repair_plan(
            state,
            user,
            question,
            registry,
            &plan,
            "semantic.actual_gen_duid has partial DUID coverage and is not suitable for whole-of-market or broad fuel-mix breakdowns. Use semantic.daily_unit_dispatch joined to semantic.unit_dimension, and if needed convert TOTALCLEARED to dispatch-energy with * 0.5 while clearly noting it is a dispatch proxy rather than metered generation.",
            &serde_json::json!({}),
            run_id,
        )
        .await?,
    );

    if plan_uses_actual_gen_duid(&repaired) {
        return Ok(Plan {
            status: "blocked".to_string(),
            sql: String::new(),
            used_objects: repaired.used_objects,
            data_description: "The metered actual-generation surface currently available in the warehouse has partial DUID coverage and is not reliable for whole-of-market fuel-mix breakdowns.".to_string(),
            note: "Blocked because the current plan still relied on a partial actual-generation surface that would materially undercount major fuels. Use the dispatch surface instead for a proxy answer, or add a fuller metered-generation dataset.".to_string(),
            chart_title: repaired.chart_title,
            confidence: "low".to_string(),
            reason: "semantic.actual_gen_duid is unsuitable for broad fuel-mix breakdowns because its coverage is partial.".to_string(),
        });
    }

    Ok(repaired)
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

fn question_requests_generation_mix(question: &str) -> bool {
    let lowered = question.to_ascii_lowercase();
    let has_fuel = lowered.contains("fuel");
    let has_mix_shape = [
        "mix",
        "breakdown",
        "by fuel",
        "fuel type",
        "source",
        "generation by",
        "each day",
        "per day",
        "daily",
    ]
    .iter()
    .any(|pattern| lowered.contains(pattern));
    let whole_market_shape = [
        "nem",
        "market",
        "last week",
        "this week",
        "yesterday",
        "today",
        "each day",
        "per day",
        "daily",
        "show me",
    ]
    .iter()
    .any(|pattern| lowered.contains(pattern));

    (has_fuel && has_mix_shape) || (has_fuel && whole_market_shape)
}

fn plan_uses_actual_gen_duid(plan: &Plan) -> bool {
    plan.used_objects
        .iter()
        .any(|item| item == "semantic.actual_gen_duid")
}

fn build_chart_spec(preview: &QueryPreview, title: &str) -> ChartSpec {
    if preview.row_count <= 1 {
        return ChartSpec::Summary {
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
        let x = preview.columns[idx].clone();
        let y = numeric.into_iter().take(2).collect::<Vec<_>>();
        return build_vega_lite_chart(
            preview,
            title,
            "line",
            x,
            y,
            choose_color_category(preview, &categorical, None),
            false,
            false,
        );
    }
    if !categorical.is_empty() && !numeric.is_empty() {
        let primary = choose_primary_category(&categorical);
        let color = choose_color_category(preview, &categorical, Some(&primary));
        let horizontal = should_use_horizontal_ranking(preview, &primary);
        return build_vega_lite_chart(
            preview,
            title,
            "bar",
            primary,
            vec![numeric[0].clone()],
            color,
            false,
            horizontal,
        );
    }
    ChartSpec::Table {
        title: title.to_string(),
    }
}

fn validate_chart_spec(chart: &ChartSpec, preview: &QueryPreview) -> Result<()> {
    match chart {
        ChartSpec::Summary { .. } | ChartSpec::Table { .. } => Ok(()),
        ChartSpec::VegaLite { spec, .. } => {
            let mut fields = Vec::new();
            collect_chart_fields(spec, &mut fields)?;
            let allowed = preview
                .columns
                .iter()
                .cloned()
                .chain(["value".to_string(), "series".to_string()])
                .collect::<HashSet<_>>();
            let invalid = fields
                .into_iter()
                .filter(|field| !allowed.contains(field))
                .collect::<Vec<_>>();
            if !invalid.is_empty() {
                bail!("chart referenced unknown fields: {}", invalid.join(", "));
            }
            Ok(())
        }
    }
}

fn collect_chart_fields(value: &Value, fields: &mut Vec<String>) -> Result<()> {
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                if key == "field" {
                    if let Some(field) = child.as_str() {
                        fields.push(field.to_string());
                    }
                } else if key == "fold" {
                    let Some(items) = child.as_array() else {
                        bail!("chart transform.fold must be an array");
                    };
                    for item in items {
                        let Some(field) = item.as_str() else {
                            bail!("chart transform.fold entries must be strings");
                        };
                        fields.push(field.to_string());
                    }
                } else {
                    collect_chart_fields(child, fields)?;
                }
            }
            Ok(())
        }
        Value::Array(items) => {
            for item in items {
                collect_chart_fields(item, fields)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn build_vega_lite_chart(
    preview: &QueryPreview,
    title: &str,
    mark_type: &str,
    x: String,
    y: Vec<String>,
    color: Option<String>,
    stacked: bool,
    horizontal: bool,
) -> ChartSpec {
    let spec = build_vega_lite_spec(
        preview,
        title,
        mark_type,
        &x,
        &y,
        color.as_deref(),
        stacked,
        horizontal,
    );
    ChartSpec::VegaLite {
        title: title.to_string(),
        spec,
    }
}

fn build_vega_lite_spec(
    preview: &QueryPreview,
    title: &str,
    mark_type: &str,
    x: &str,
    y: &[String],
    color: Option<&str>,
    stacked: bool,
    horizontal: bool,
) -> Value {
    let mut encoding = serde_json::Map::new();

    let mut transforms = Vec::new();
    let y_field = if y.len() > 1 {
        transforms.push(json!({
            "fold": y,
            "as": ["series", "value"]
        }));
        "value".to_string()
    } else {
        y.first().cloned().unwrap_or_else(|| "value".to_string())
    };
    let y_title = if y.len() > 1 {
        "Value".to_string()
    } else {
        axis_title_for_column(preview, &y_field)
    };
    if horizontal {
        encoding.insert(
            "x".to_string(),
            json!({
                "field": y_field,
                "type": "quantitative",
                "title": y_title,
                "stack": if mark_type == "bar" && stacked { json!("zero") } else { Value::Null },
            }),
        );
        encoding.insert(
            "y".to_string(),
            json!({
                "field": x,
                "type": vega_field_type(preview, x),
                "title": axis_title_for_column(preview, x),
                "sort": { "field": y_field, "order": "descending" },
            }),
        );
    } else {
        encoding.insert(
            "x".to_string(),
            json!({
                "field": x,
                "type": vega_field_type(preview, x),
                "title": axis_title_for_column(preview, x),
            }),
        );
        encoding.insert(
            "y".to_string(),
            json!({
                "field": y_field,
                "type": "quantitative",
                "title": y_title,
                "stack": if mark_type == "bar" && stacked { json!("zero") } else { Value::Null },
            }),
        );
    }

    let color_field = if y.len() > 1 {
        Some("series".to_string())
    } else {
        color.map(|value| value.to_string())
    };
    if let Some(field) = color_field {
        let title = prettify_column(&field);
        encoding.insert(
            "color".to_string(),
            json!({
                "field": field,
                "type": "nominal",
                "title": title,
                "scale": {
                    "range": ["#60a5fa", "#f59e0b", "#34d399", "#f472b6", "#a78bfa", "#f87171", "#22d3ee", "#84cc16"]
                }
            }),
        );
    }
    encoding.insert(
        "tooltip".to_string(),
        json!(
            preview
                .columns
                .iter()
                .map(|column| {
                    json!({
                        "field": column,
                        "type": vega_field_type(preview, column),
                        "title": axis_title_for_column(preview, column),
                    })
                })
                .collect::<Vec<_>>()
        ),
    );

    let mark = if mark_type == "line" {
        json!({ "type": "line", "point": true, "strokeWidth": 2.5 })
    } else {
        json!({ "type": mark_type, "cornerRadiusTopLeft": 2, "cornerRadiusTopRight": 2 })
    };

    let mut spec = serde_json::Map::new();
    spec.insert(
        "$schema".to_string(),
        json!("https://vega.github.io/schema/vega-lite/v6.json"),
    );
    spec.insert("title".to_string(), json!(title));
    spec.insert("mark".to_string(), mark);
    spec.insert("encoding".to_string(), Value::Object(encoding));
    spec.insert("width".to_string(), json!("container"));
    spec.insert("height".to_string(), json!(320));
    spec.insert(
        "config".to_string(),
        json!({
            "background": "transparent",
            "view": { "stroke": null },
            "axis": {
                "labelColor": "#a3a3a3",
                "titleColor": "#a3a3a3",
                "gridColor": "rgba(64,64,64,0.45)",
                "domainColor": "rgba(82,82,82,0.7)"
            },
            "legend": {
                "labelColor": "#a3a3a3",
                "titleColor": "#a3a3a3"
            },
            "title": {
                "color": "#f5f5f5",
                "font": "Space Grotesk, sans-serif",
                "anchor": "start",
                "fontSize": 16
            }
        }),
    );
    if !transforms.is_empty() {
        spec.insert("transform".to_string(), Value::Array(transforms));
    }
    Value::Object(spec)
}

fn compact_thread_context(messages: &[ThreadContextMessage]) -> Value {
    Value::Array(
        messages
            .iter()
            .rev()
            .take(8)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .map(|message| {
                let chart = message
                    .metadata
                    .get("chart")
                    .cloned()
                    .unwrap_or(Value::Null);
                let preview = message
                    .metadata
                    .get("preview")
                    .cloned()
                    .and_then(|value| serde_json::from_value::<QueryPreview>(value).ok())
                    .map(|preview| {
                        serde_json::json!({
                            "columns": preview.columns,
                            "row_count": preview.row_count,
                            "sample_rows": preview.rows.into_iter().take(6).collect::<Vec<_>>(),
                        })
                    })
                    .unwrap_or(Value::Null);
                serde_json::json!({
                    "role": message.role,
                    "content": message.content,
                    "sql_text": message.sql_text,
                    "chart": chart,
                    "preview": preview,
                })
            })
            .collect(),
    )
}

fn maybe_build_visualization_follow_up(
    question: &str,
    messages: &[ThreadContextMessage],
) -> Option<(Plan, FinalAnswer, ChartSpec, QueryPreview)> {
    let follow_up = parse_chart_follow_up(question)?;
    let prior = messages.iter().rev().find_map(extract_preview_and_chart)?;
    let (preview, mut chart) = prior;

    match follow_up {
        ChartFollowUp::StackedBar => {
            chart = restyle_chart_as_stacked_bar(&preview, chart)?;
        }
    }

    let plan = Plan {
        status: "answerable".to_string(),
        sql: String::new(),
        used_objects: Vec::new(),
        data_description: "Reused the previous query result from this conversation and changed only the visualization.".to_string(),
        note: "No new SQL was run. This chart reuses the prior result from the thread.".to_string(),
        chart_title: chart_title(&chart).to_string(),
        confidence: "high".to_string(),
        reason: "The user requested a visualization change on the previous result set.".to_string(),
    };
    let answer = FinalAnswer {
        answer: "Updated the previous result to a stacked bar chart.".to_string(),
        note:
            "Reused the prior query result from this thread without rerunning the warehouse query."
                .to_string(),
    };
    Some((plan, answer, chart, preview))
}

fn extract_preview_and_chart(message: &ThreadContextMessage) -> Option<(QueryPreview, ChartSpec)> {
    if message.role != "assistant" {
        return None;
    }
    let preview =
        serde_json::from_value::<QueryPreview>(message.metadata.get("preview")?.clone()).ok()?;
    let chart = serde_json::from_value::<ChartSpec>(message.metadata.get("chart")?.clone()).ok()?;
    Some((preview, chart))
}

fn restyle_chart_as_stacked_bar(preview: &QueryPreview, chart: ChartSpec) -> Option<ChartSpec> {
    match chart {
        ChartSpec::VegaLite { title, mut spec } => {
            let x = vega_encoding_field(&spec, "x").or_else(|| infer_chart_x(preview))?;
            let y = vega_encoding_field(&spec, "y")
                .map(|field| vec![field])
                .or_else(|| {
                    let inferred = infer_chart_y(preview);
                    if inferred.is_empty() {
                        None
                    } else {
                        Some(inferred)
                    }
                })?;
            let color =
                vega_encoding_field(&spec, "color").or_else(|| infer_chart_color(preview, &x, &y));
            spec = build_vega_lite_spec(
                preview,
                &format!("{title} (Stacked)"),
                "bar",
                &x,
                &y,
                color.as_deref(),
                true,
                false,
            );
            Some(ChartSpec::VegaLite {
                title: format!("{title} (Stacked)"),
                spec,
            })
        }
        ChartSpec::Summary { title } | ChartSpec::Table { title } => {
            let x = infer_chart_x(preview)?;
            let y = infer_chart_y(preview);
            if y.is_empty() {
                return None;
            }
            let color = infer_chart_color(preview, &x, &y);
            Some(build_vega_lite_chart(
                preview,
                &format!("{title} (Stacked)"),
                "bar",
                x,
                y,
                color,
                true,
                false,
            ))
        }
    }
}

fn vega_encoding_field(spec: &Value, channel: &str) -> Option<String> {
    spec.get("encoding")?
        .get(channel)?
        .get("field")?
        .as_str()
        .map(str::to_string)
}

fn chart_title(chart: &ChartSpec) -> &str {
    match chart {
        ChartSpec::Summary { title }
        | ChartSpec::Table { title }
        | ChartSpec::VegaLite { title, .. } => title,
    }
}

enum ChartFollowUp {
    StackedBar,
}

fn parse_chart_follow_up(question: &str) -> Option<ChartFollowUp> {
    let lowered = question.to_ascii_lowercase();
    let refers_to_prior = [
        "previous",
        "prior",
        "same",
        "that chart",
        "that graph",
        "this chart",
        "this graph",
    ]
    .iter()
    .any(|pattern| lowered.contains(pattern));
    let mentions_visual = ["chart", "graph", "plot", "visual"]
        .iter()
        .any(|pattern| lowered.contains(pattern));
    if (refers_to_prior || mentions_visual)
        && lowered.contains("stacked")
        && lowered.contains("bar")
    {
        return Some(ChartFollowUp::StackedBar);
    }
    None
}

fn infer_chart_x(preview: &QueryPreview) -> Option<String> {
    preview
        .columns
        .iter()
        .find(|column| {
            is_timeish_name(column) || column_values_look_like_datetimes(preview, column)
        })
        .cloned()
        .or_else(|| {
            preview
                .columns
                .iter()
                .find(|column| !column_values_look_numeric(preview, column))
                .cloned()
        })
}

fn infer_chart_y(preview: &QueryPreview) -> Vec<String> {
    preview
        .columns
        .iter()
        .filter(|column| column_values_look_numeric(preview, column))
        .take(2)
        .cloned()
        .collect()
}

fn infer_chart_color(preview: &QueryPreview, x: &str, y: &[String]) -> Option<String> {
    preview
        .columns
        .iter()
        .find(|column| {
            column.as_str() != x
                && !y.iter().any(|item| item == *column)
                && !column_values_look_numeric(preview, column)
        })
        .cloned()
}

fn choose_primary_category(categorical: &[String]) -> String {
    categorical
        .iter()
        .find(|column| looks_like_name(column))
        .cloned()
        .or_else(|| {
            categorical
                .iter()
                .find(|column| !looks_like_identifier(column))
                .cloned()
        })
        .unwrap_or_else(|| categorical[0].clone())
}

fn choose_color_category(
    preview: &QueryPreview,
    categorical: &[String],
    primary: Option<&str>,
) -> Option<String> {
    let preferred = ["REGIONID", "FUEL_TYPE", "PARTICIPANTID"];
    preferred
        .iter()
        .find_map(|wanted| {
            categorical.iter().find(|column| {
                column.as_str() != primary.unwrap_or_default()
                    && column.as_str() == *wanted
                    && color_cardinality_is_reasonable(preview, column)
            })
        })
        .cloned()
        .or_else(|| {
            categorical
                .iter()
                .find(|column| {
                    column.as_str() != primary.unwrap_or_default()
                        && !looks_like_name(column)
                        && color_cardinality_is_reasonable(preview, column)
                })
                .cloned()
        })
}

fn should_use_horizontal_ranking(preview: &QueryPreview, category: &str) -> bool {
    let cardinality = column_cardinality(preview, category);
    cardinality >= 8 || cardinality.saturating_mul(2) >= preview.row_count
}

fn color_cardinality_is_reasonable(preview: &QueryPreview, column: &str) -> bool {
    let cardinality = column_cardinality(preview, column);
    (2..=8).contains(&cardinality)
}

fn column_cardinality(preview: &QueryPreview, column: &str) -> usize {
    let Some(idx) = preview.columns.iter().position(|item| item == column) else {
        return 0;
    };
    let mut seen = HashSet::new();
    for row in &preview.rows {
        if let Some(value) = row.get(idx) {
            seen.insert(value.to_string());
        }
    }
    seen.len()
}

fn looks_like_identifier(column: &str) -> bool {
    matches!(column, "DUID" | "PARTICIPANTID" | "REGIONID") || column.ends_with("ID")
}

fn looks_like_name(column: &str) -> bool {
    column.contains("NAME") || column.ends_with("NAME")
}

fn vega_field_type(preview: &QueryPreview, column: &str) -> &'static str {
    if is_timeish_name(column) || column_values_look_like_datetimes(preview, column) {
        "temporal"
    } else if column_values_look_numeric(preview, column) {
        "quantitative"
    } else {
        "nominal"
    }
}

fn axis_title_for_column(preview: &QueryPreview, column: &str) -> String {
    let unit = infer_unit(column);
    let is_datetime = is_timeish_name(column) || column_values_look_like_datetimes(preview, column);
    if is_datetime {
        format!("{} (UTC)", prettify_column(column))
    } else if let Some(unit) = unit {
        format!("{} ({unit})", prettify_column(column))
    } else {
        prettify_column(column)
    }
}

fn infer_unit(column: &str) -> Option<&'static str> {
    let lowered = column.to_ascii_lowercase();
    if lowered.contains("mwh") || lowered.contains("energy") {
        Some("MWh")
    } else if lowered.contains("rrp")
        || lowered.contains("rop")
        || lowered.contains("eep")
        || lowered.contains("price")
        || lowered.contains("marginalvalue")
    {
        Some("AUD/MWh")
    } else if lowered.contains("demand") || lowered.contains("mw") || lowered.contains("flow") {
        Some("MW")
    } else if lowered.contains("percent") || lowered.ends_with("pct") {
        Some("%")
    } else {
        None
    }
}

fn prettify_column(column: &str) -> String {
    column
        .replace('_', " ")
        .to_ascii_lowercase()
        .split_whitespace()
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                Some(first) => format!("{}{}", first.to_ascii_uppercase(), chars.as_str()),
                None => String::new(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
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

    use super::{
        Plan, QueryPreview, build_chart_spec, enforce_semantic_contract,
        maybe_build_visualization_follow_up, validate_sql,
    };
    use crate::models::{ChartSpec, ThreadContextMessage};

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
        assert!(matches!(chart, ChartSpec::Summary { .. }));
    }

    #[test]
    fn previous_result_can_be_recast_as_stacked_bar() {
        let context = vec![ThreadContextMessage {
            role: "assistant".to_string(),
            content: "Here is the daily fuel breakdown.".to_string(),
            sql_text: Some("SELECT ...".to_string()),
            metadata: json!({
                "chart": {
                    "renderer": "vega_lite",
                    "title": "Daily Energy by Fuel Type",
                    "spec": {
                        "$schema": "https://vega.github.io/schema/vega-lite/v6.json",
                        "mark": { "type": "line", "point": true },
                        "encoding": {
                            "x": { "field": "day", "type": "temporal" },
                            "y": { "field": "mwh", "type": "quantitative" },
                            "color": { "field": "fuel_type", "type": "nominal" }
                        }
                    }
                },
                "preview": {
                    "columns": ["day", "fuel_type", "mwh"],
                    "rows": [
                        ["2026-03-02", "Black coal", 1528867.0],
                        ["2026-03-02", "Wind", 491045.0]
                    ],
                    "row_count": 2
                }
            }),
        }];

        let Some((_, answer, chart, preview)) = maybe_build_visualization_follow_up(
            "give the previous graph as a stacked bar graph",
            &context,
        ) else {
            panic!("expected chart follow-up");
        };

        assert_eq!(
            answer.answer,
            "Updated the previous result to a stacked bar chart."
        );
        match chart {
            ChartSpec::VegaLite { spec, .. } => {
                assert_eq!(
                    spec.pointer("/mark/type").and_then(|value| value.as_str()),
                    Some("bar")
                );
                assert_eq!(
                    spec.pointer("/encoding/y/stack")
                        .and_then(|value| value.as_str()),
                    Some("zero")
                );
                assert_eq!(
                    spec.pointer("/encoding/x/field")
                        .and_then(|value| value.as_str()),
                    Some("day")
                );
                assert_eq!(
                    spec.pointer("/encoding/color/field")
                        .and_then(|value| value.as_str()),
                    Some("fuel_type")
                );
            }
            _ => panic!("expected vega-lite chart"),
        }
        assert_eq!(preview.row_count, 2);
    }
}
