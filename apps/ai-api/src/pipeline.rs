use std::{collections::HashSet, time::Instant};

use anyhow::{Result, anyhow, bail};
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
const MAX_ANSWER_ATTEMPTS: usize = 2;
const PLAN_MAX_TOKENS: u32 = 1600;
const REPAIR_MAX_TOKENS: u32 = 1600;
const ANSWER_MAX_TOKENS: u32 = 400;

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
    let filtered_registry = filter_registry_for_question(&request.question, &registry);

    emit_status(&sender, "planning", "Planning query").await;
    let mut plan = enforce_semantic_contract(
        &request.question,
        plan_question(
            &state,
            &user,
            &request.question,
            &filtered_registry,
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
        &filtered_registry,
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
                &filtered_registry,
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
            &filtered_registry,
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

    emit_status(&sender, "chart", "Building chart").await;
    let chart = match build_chart_spec_for_plan(&preview, &plan) {
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
        .json_completion_with_max_tokens(
            &prompts::planner_system_prompt(),
            &prompts::planner_user_prompt(question, registry, approved_proposal, thread_context),
            PLAN_MAX_TOKENS,
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
        .json_completion_with_max_tokens(
            &prompts::repair_system_prompt(),
            &prompts::repair_user_prompt(question, registry, plan, error, thread_context),
            REPAIR_MAX_TOKENS,
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
    let mut invalid_answer = None::<String>;
    let mut violation = None::<String>;

    for attempt in 0..MAX_ANSWER_ATTEMPTS {
        let (system_prompt, user_prompt, prompt_type) =
            if let (Some(previous), Some(reason)) = (&invalid_answer, &violation) {
                (
                    prompts::answer_repair_system_prompt(),
                    prompts::answer_repair_user_prompt(
                        question,
                        plan,
                        &preview.columns,
                        &preview.rows,
                        thread_context,
                        previous,
                        reason,
                    ),
                    "answer_repair",
                )
            } else {
                (
                    prompts::answer_system_prompt(),
                    prompts::answer_user_prompt(
                        question,
                        plan,
                        &preview.columns,
                        &preview.rows,
                        thread_context,
                    ),
                    "answer",
                )
            };

        let response = state
            .llm
            .json_completion_with_max_tokens(system_prompt, &user_prompt, ANSWER_MAX_TOKENS)
            .await?;
        log_llm_usage(state, user, run_id, prompt_type, &response).await?;
        let answer = extract_json::<FinalAnswer>(&response.text)?;
        if let Err(reason) = validate_final_answer(&answer) {
            invalid_answer = Some(answer.answer);
            violation = Some(reason.to_string());
            if attempt + 1 < MAX_ANSWER_ATTEMPTS {
                continue;
            }
            bail!("answer validation failed: {reason}");
        }
        return Ok(answer);
    }

    bail!("answer generation failed")
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
            chart_type: repaired.chart_type,
            x: repaired.x,
            y: repaired.y,
            y2: repaired.y2,
            color: repaired.color,
            y_label: repaired.y_label,
            y2_label: repaired.y2_label,
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
            chart_type: plan.chart_type,
            x: plan.x,
            y: plan.y,
            y2: plan.y2,
            color: plan.color,
            y_label: plan.y_label,
            y2_label: plan.y2_label,
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

fn filter_registry_for_question(question: &str, registry: &[SemanticObject]) -> Vec<SemanticObject> {
    let lowered_question = question.to_ascii_lowercase();
    let tokens = lowered_question
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|token| token.len() >= 3)
        .collect::<Vec<_>>();

    let mut scored = registry
        .iter()
        .map(|item| {
            let haystack = format!(
                "{} {} {} {} {} {} {} {}",
                item.object_name,
                item.description,
                item.grain,
                item.time_column,
                item.dimensions.join(" "),
                item.measures.join(" "),
                item.join_keys.join(" "),
                item.question_tags.join(" ")
            )
            .to_ascii_lowercase();
            let mut score = 0usize;
            for token in &tokens {
                if haystack.contains(*token) {
                    score += 1;
                }
            }
            for tag in &item.question_tags {
                let tag = tag.to_ascii_lowercase();
                if !tag.is_empty() && lowered_question.contains(&tag) {
                    score += 3;
                }
            }
            (score, item.clone())
        })
        .collect::<Vec<_>>();

    scored.sort_by(|a, b| {
        b.0.cmp(&a.0)
            .then_with(|| a.1.object_name.cmp(&b.1.object_name))
    });

    let selected = scored
        .into_iter()
        .filter(|(score, _)| *score > 0)
        .take(14)
        .map(|(_, item)| item)
        .collect::<Vec<_>>();

    if selected.is_empty() {
        registry.iter().take(14).cloned().collect()
    } else {
        selected
    }
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
        return build_plotly_chart(
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
        return build_plotly_chart(
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

fn build_chart_spec_for_plan(preview: &QueryPreview, plan: &Plan) -> Result<ChartSpec> {
    let chart_type = plan.chart_type.trim().to_ascii_lowercase();
    if chart_type.is_empty() {
        bail!("plan omitted chart_type");
    }
    if chart_type == "summary" {
        return Ok(ChartSpec::Summary {
            title: plan.chart_title.clone(),
        });
    }
    if chart_type == "table" {
        return Ok(ChartSpec::Table {
            title: plan.chart_title.clone(),
        });
    }

    let x = plan
        .x
        .clone()
        .or_else(|| infer_chart_x(preview))
        .ok_or_else(|| anyhow!("plan omitted x axis"))?;
    let y = if plan.y.is_empty() {
        infer_chart_y(preview)
    } else {
        plan.y.clone()
    };
    if y.is_empty() {
        bail!("plan omitted y axis");
    }

    match chart_type.as_str() {
        "line" | "bar" | "scatter" | "area" => Ok(build_plotly_chart_from_plan(preview, plan, &x, &y)),
        "pie" => Ok(build_plotly_pie_chart(preview, plan, &x, &y[0])),
        "box" => Ok(build_plotly_box_chart(preview, plan, &x, &y[0])),
        _ => bail!("unsupported chart_type `{chart_type}`"),
    }
}

fn validate_chart_spec(chart: &ChartSpec, preview: &QueryPreview) -> Result<()> {
    match chart {
        ChartSpec::Summary { .. } | ChartSpec::Table { .. } => Ok(()),
        ChartSpec::Plotly { figure, .. } => {
            let Some(data) = figure.get("data").and_then(Value::as_array) else {
                bail!("plotly figure must contain a data array");
            };
            if data.is_empty() {
                bail!("plotly figure data array cannot be empty");
            }
            let _ = preview;
            Ok(())
        }
    }
}

fn build_plotly_chart(
    preview: &QueryPreview,
    title: &str,
    mark_type: &str,
    x: String,
    y: Vec<String>,
    color: Option<String>,
    stacked: bool,
    horizontal: bool,
) -> ChartSpec {
    let figure = build_plotly_figure(
        preview,
        title,
        mark_type,
        &x,
        &y,
        color.as_deref(),
        stacked,
        horizontal,
    );
    ChartSpec::Plotly {
        title: title.to_string(),
        figure,
    }
}

fn build_plotly_chart_from_plan(
    preview: &QueryPreview,
    plan: &Plan,
    x: &str,
    y: &[String],
) -> ChartSpec {
    let mark_type = plan.chart_type.trim().to_ascii_lowercase();
    let stacked = mark_type == "area";
    let figure = build_plotly_figure(
        preview,
        &plan.chart_title,
        &mark_type,
        x,
        y,
        plan.color.as_deref(),
        stacked,
        false,
    );
    let mut figure = figure;
    if let Some(layout) = figure.get_mut("layout").and_then(Value::as_object_mut) {
        if let Some(y_label) = &plan.y_label {
            layout.insert(
                "yaxis".to_string(),
                merge_axis_title(layout.get("yaxis").cloned(), y_label),
            );
        }
        if let Some(xaxis) = layout.get_mut("xaxis").and_then(Value::as_object_mut) {
            if let Some(title) = xaxis.get_mut("title").and_then(Value::as_object_mut) {
                if title.get("text").is_none() {
                    title.insert("text".to_string(), json!(axis_title_for_column(preview, x)));
                }
            }
        }
    }
    ChartSpec::Plotly {
        title: plan.chart_title.clone(),
        figure,
    }
}

fn build_plotly_pie_chart(preview: &QueryPreview, plan: &Plan, x: &str, y: &str) -> ChartSpec {
    let rows = preview_rows_as_objects(preview);
    let labels = rows
        .iter()
        .map(|row| row.get(x).cloned().unwrap_or(Value::Null))
        .collect::<Vec<_>>();
    let values = rows
        .iter()
        .map(|row| row.get(y).cloned().unwrap_or(Value::Null))
        .collect::<Vec<_>>();
    ChartSpec::Plotly {
        title: plan.chart_title.clone(),
        figure: json!({
            "data": [{
                "type": "pie",
                "labels": labels,
                "values": values,
                "textinfo": "label+percent",
                "hovertemplate": format!("{}: %{{label}}<br>{}: %{{value}}<extra></extra>", prettify_column(x), plan.y_label.clone().unwrap_or_else(|| axis_title_for_column(preview, y)))
            }],
            "layout": base_layout(&plan.chart_title, None, None, false, false),
            "config": base_config()
        }),
    }
}

fn build_plotly_box_chart(preview: &QueryPreview, plan: &Plan, x: &str, y: &str) -> ChartSpec {
    let rows = preview_rows_as_objects(preview);
    let groups = group_rows_by(&rows, x);
    let palette = [
        "#60a5fa", "#f59e0b", "#34d399", "#f472b6", "#a78bfa", "#f87171", "#22d3ee", "#84cc16",
    ];
    let traces = groups
        .into_iter()
        .enumerate()
        .map(|(index, (group, group_rows))| {
            let values = group_rows
                .iter()
                .map(|row| row.get(y).cloned().unwrap_or(Value::Null))
                .collect::<Vec<_>>();
            json!({
                "type": "box",
                "name": group,
                "y": values,
                "marker": { "color": palette[index % palette.len()] }
            })
        })
        .collect::<Vec<_>>();
    ChartSpec::Plotly {
        title: plan.chart_title.clone(),
        figure: json!({
            "data": traces,
            "layout": base_layout(
                &plan.chart_title,
                None,
                Some(plan.y_label.clone().unwrap_or_else(|| axis_title_for_column(preview, y))),
                false,
                false
            ),
            "config": base_config()
        }),
    }
}

fn build_plotly_figure(
    preview: &QueryPreview,
    title: &str,
    mark_type: &str,
    x: &str,
    y: &[String],
    color: Option<&str>,
    stacked: bool,
    horizontal: bool,
) -> Value {
    let palette = [
        "#60a5fa", "#f59e0b", "#34d399", "#f472b6", "#a78bfa", "#f87171", "#22d3ee", "#84cc16",
    ];
    let rows = preview_rows_as_objects(preview);
    let color_field = if y.len() > 1 {
        Some("series".to_string())
    } else {
        color.map(str::to_string)
    };

    let traces = if y.len() > 1 {
        y.iter()
            .enumerate()
            .map(|(index, y_field)| {
                let x_values = rows
                    .iter()
                    .map(|row| row.get(x).cloned().unwrap_or(Value::Null))
                    .collect::<Vec<_>>();
                let y_values = rows
                    .iter()
                    .map(|row| row.get(y_field).cloned().unwrap_or(Value::Null))
                    .collect::<Vec<_>>();
                base_plotly_trace(
                    mark_type,
                    horizontal,
                    &x_values,
                    &y_values,
                    &prettify_column(y_field),
                    palette[index % palette.len()],
                    preview
                        .columns
                        .iter()
                        .map(|column| row_column_values(&rows, column))
                        .collect::<Vec<_>>(),
                    &preview.columns,
                )
            })
            .collect::<Vec<_>>()
    } else if let Some(color_field) = color_field {
        let groups = group_rows_by(&rows, &color_field);
        groups
            .into_iter()
            .enumerate()
            .map(|(index, (group, group_rows))| {
                let x_values = group_rows
                    .iter()
                    .map(|row| row.get(x).cloned().unwrap_or(Value::Null))
                    .collect::<Vec<_>>();
                let y_values = group_rows
                    .iter()
                    .map(|row| row.get(&y[0]).cloned().unwrap_or(Value::Null))
                    .collect::<Vec<_>>();
                base_plotly_trace(
                    mark_type,
                    horizontal,
                    &x_values,
                    &y_values,
                    &group,
                    palette[index % palette.len()],
                    preview
                        .columns
                        .iter()
                        .map(|column| row_column_values(&group_rows, column))
                        .collect::<Vec<_>>(),
                    &preview.columns,
                )
            })
            .collect::<Vec<_>>()
    } else {
        let x_values = rows
            .iter()
            .map(|row| row.get(x).cloned().unwrap_or(Value::Null))
            .collect::<Vec<_>>();
        let y_values = rows
            .iter()
            .map(|row| row.get(&y[0]).cloned().unwrap_or(Value::Null))
            .collect::<Vec<_>>();
        vec![base_plotly_trace(
            mark_type,
            horizontal,
            &x_values,
            &y_values,
            &prettify_column(&y[0]),
            palette[0],
            preview
                .columns
                .iter()
                .map(|column| row_column_values(&rows, column))
                .collect::<Vec<_>>(),
            &preview.columns,
        )]
    };

    json!({
        "data": traces,
        "layout": base_layout(
            title,
            Some(if horizontal { axis_title_for_column(preview, &y[0]) } else { axis_title_for_column(preview, x) }),
            Some(if horizontal { axis_title_for_column(preview, x) } else { axis_title_for_column(preview, &y[0]) }),
            stacked,
            horizontal
        ),
        "config": base_config()
    })
}

fn base_layout(
    title: &str,
    x_title: Option<String>,
    y_title: Option<String>,
    stacked: bool,
    horizontal: bool,
) -> Value {
    json!({
        "title": { "text": title, "x": 0.02, "font": { "family": "Space Grotesk, sans-serif", "color": "#f5f5f5", "size": 16 } },
        "paper_bgcolor": "rgba(0,0,0,0)",
        "plot_bgcolor": "rgba(0,0,0,0)",
        "font": { "family": "Space Grotesk, sans-serif", "color": "#d4d4d4" },
        "height": 360,
        "margin": { "t": 56, "r": 24, "b": 44, "l": if horizontal { 140 } else { 52 } },
        "legend": { "orientation": "h", "x": 0, "y": 1.12, "font": { "color": "#a3a3a3" } },
        "barmode": if stacked { "stack" } else { "group" },
        "xaxis": {
            "title": { "text": x_title, "font": { "color": "#a3a3a3", "size": 12 } },
            "gridcolor": "rgba(64,64,64,0.45)",
            "zerolinecolor": "rgba(64,64,64,0.45)",
            "linecolor": "rgba(82,82,82,0.7)",
            "automargin": true
        },
        "yaxis": {
            "title": { "text": y_title, "font": { "color": "#a3a3a3", "size": 12 } },
            "gridcolor": "rgba(64,64,64,0.45)",
            "zerolinecolor": "rgba(64,64,64,0.45)",
            "linecolor": "rgba(82,82,82,0.7)",
            "automargin": true
        }
    })
}

fn base_config() -> Value {
    json!({
        "responsive": true,
        "displaylogo": false,
        "modeBarButtonsToRemove": ["lasso2d", "select2d"]
    })
}

fn merge_axis_title(existing_axis: Option<Value>, label: &str) -> Value {
    let mut axis = existing_axis.unwrap_or_else(|| json!({}));
    if let Some(object) = axis.as_object_mut() {
        object.insert(
            "title".to_string(),
            json!({ "text": label, "font": { "color": "#a3a3a3", "size": 12 } }),
        );
    }
    axis
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
        chart_type: "bar".to_string(),
        x: None,
        y: Vec::new(),
        y2: Vec::new(),
        color: None,
        y_label: None,
        y2_label: None,
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
        ChartSpec::Plotly { title, figure } => Some(ChartSpec::Plotly {
            title: format!("{title} (Stacked)"),
            figure: restyle_plotly_figure_as_stacked_bar(figure, &format!("{title} (Stacked)")),
        }),
        ChartSpec::Summary { title } | ChartSpec::Table { title } => {
            let x = infer_chart_x(preview)?;
            let y = infer_chart_y(preview);
            if y.is_empty() {
                return None;
            }
            let color = infer_chart_color(preview, &x, &y);
            Some(build_plotly_chart(
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

fn chart_title(chart: &ChartSpec) -> &str {
    match chart {
        ChartSpec::Summary { title }
        | ChartSpec::Table { title }
        | ChartSpec::Plotly { title, .. } => title,
    }
}

fn validate_final_answer(answer: &FinalAnswer) -> Result<()> {
    let text = answer.answer.trim();
    if text.is_empty() {
        bail!("answer was empty");
    }

    let lower = text.to_ascii_lowercase();
    let forbidden_phrases = [
        "python snippet",
        "matplotlib",
        "pandas",
        "plt.",
        "import pandas",
        "import matplotlib",
        "fig, ax",
        "plt.show()",
        "run the following",
        "use the following python",
        "plot the following",
        "the chart shows",
        "the table contains",
        "total rows",
        "to produce",
    ];
    if let Some(phrase) = forbidden_phrases
        .iter()
        .find(|phrase| lower.contains(**phrase))
    {
        bail!("answer contained forbidden phrase `{phrase}`");
    }

    if text.contains("```") {
        bail!("answer contained a code block");
    }

    let suspicious_code = Regex::new(
        r"(?m)^\s*(import\s+\w+|from\s+\w+\s+import|df\s*=|fig\s*,\s*ax\s*=|ax\.\w+\(|plt\.\w+\()",
    )
    .expect("valid code regex");
    if suspicious_code.is_match(text) {
        bail!("answer contained code-like instructions");
    }

    Ok(())
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

fn preview_rows_as_objects(preview: &QueryPreview) -> Vec<serde_json::Map<String, Value>> {
    preview
        .rows
        .iter()
        .map(|row| {
            preview
                .columns
                .iter()
                .enumerate()
                .map(|(index, column)| {
                    (
                        column.clone(),
                        row.get(index).cloned().unwrap_or(Value::Null),
                    )
                })
                .collect::<serde_json::Map<String, Value>>()
        })
        .collect()
}

fn row_column_values(rows: &[serde_json::Map<String, Value>], column: &str) -> Vec<Value> {
    rows.iter()
        .map(|row| row.get(column).cloned().unwrap_or(Value::Null))
        .collect()
}

fn group_rows_by(
    rows: &[serde_json::Map<String, Value>],
    column: &str,
) -> Vec<(String, Vec<serde_json::Map<String, Value>>)> {
    let mut ordered = Vec::<(String, Vec<serde_json::Map<String, Value>>)>::new();
    for row in rows {
        let key = row
            .get(column)
            .map(display_value)
            .unwrap_or_else(|| "Other".to_string());
        if let Some((_, bucket)) = ordered.iter_mut().find(|(existing, _)| *existing == key) {
            bucket.push(row.clone());
        } else {
            ordered.push((key, vec![row.clone()]));
        }
    }
    ordered
}

fn base_plotly_trace(
    mark_type: &str,
    horizontal: bool,
    x_values: &[Value],
    y_values: &[Value],
    name: &str,
    color: &str,
    customdata: Vec<Vec<Value>>,
    columns: &[String],
) -> Value {
    let axis_x = if horizontal { y_values } else { x_values };
    let axis_y = if horizontal { x_values } else { y_values };
    let (trace_type, mode, orientation, fill) = match mark_type {
        "line" => ("scatter", Some("lines+markers"), None::<&str>, None::<&str>),
        "scatter" => ("scatter", Some("markers"), None::<&str>, None::<&str>),
        "area" => ("scatter", Some("lines"), None::<&str>, Some("tozeroy")),
        _ => (
            "bar",
            None::<&str>,
            if horizontal { Some("h") } else { None::<&str> },
            None::<&str>,
        ),
    };
    json!({
        "type": trace_type,
        "mode": mode,
        "orientation": orientation,
        "x": axis_x,
        "y": axis_y,
        "name": name,
        "marker": { "color": color },
        "line": { "color": color, "width": 2.5 },
        "fill": fill,
        "customdata": transpose_customdata(customdata),
        "hovertemplate": plotly_hover_template(columns),
    })
}

fn transpose_customdata(columns: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
    if columns.is_empty() {
        return Vec::new();
    }
    let row_count = columns[0].len();
    (0..row_count)
        .map(|row_index| {
            columns
                .iter()
                .map(|column| column.get(row_index).cloned().unwrap_or(Value::Null))
                .collect::<Vec<_>>()
        })
        .collect()
}

fn plotly_hover_template(columns: &[String]) -> String {
    let mut template = String::new();
    for (index, column) in columns.iter().enumerate() {
        template.push_str(&format!(
            "{}: %{{customdata[{index}]}}<br>",
            prettify_column(column)
        ));
    }
    template.push_str("<extra></extra>");
    template
}

fn restyle_plotly_figure_as_stacked_bar(mut figure: Value, title: &str) -> Value {
    if let Some(data) = figure.get_mut("data").and_then(Value::as_array_mut) {
        for trace in data {
            if let Some(object) = trace.as_object_mut() {
                object.insert("type".to_string(), json!("bar"));
                object.remove("mode");
                object.remove("orientation");
            }
        }
    }
    if let Some(layout) = figure.get_mut("layout").and_then(Value::as_object_mut) {
        layout.insert("barmode".to_string(), json!("stack"));
        layout.insert("title".to_string(), json!({ "text": title, "x": 0.02, "font": { "family": "Space Grotesk, sans-serif", "color": "#f5f5f5", "size": 16 } }));
    }
    figure
}

fn display_value(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Number(number) => number.to_string(),
        Value::Bool(flag) => flag.to_string(),
        Value::Null => "Null".to_string(),
        other => other.to_string(),
    }
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
        FinalAnswer, Plan, QueryPreview, build_chart_spec, enforce_semantic_contract,
        maybe_build_visualization_follow_up, validate_final_answer, validate_sql,
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
                chart_type: "table".to_string(),
                x: None,
                y: Vec::new(),
                y2: Vec::new(),
                color: None,
                y_label: None,
                y2_label: None,
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
                    "renderer": "plotly",
                    "title": "Daily Energy by Fuel Type",
                    "figure": {
                        "data": [
                            {
                                "type": "scatter",
                                "mode": "lines+markers",
                                "x": ["2026-03-02", "2026-03-02"],
                                "y": [1528867.0, 491045.0],
                                "name": "Fuel Type"
                            }
                        ],
                        "layout": {
                            "title": { "text": "Daily Energy by Fuel Type" }
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
            ChartSpec::Plotly { figure, .. } => {
                assert_eq!(
                    figure
                        .pointer("/data/0/type")
                        .and_then(|value| value.as_str()),
                    Some("bar")
                );
                assert_eq!(
                    figure
                        .pointer("/layout/barmode")
                        .and_then(|value| value.as_str()),
                    Some("stack")
                );
            }
            _ => panic!("expected plotly chart"),
        }
        assert_eq!(preview.row_count, 2);
    }

    #[test]
    fn answer_validation_blocks_python_plotting_instructions() {
        let error = validate_final_answer(&FinalAnswer {
            answer: "To produce a clear chart, run the following Python snippet:\nimport pandas as pd\nimport matplotlib.pyplot as plt".to_string(),
            note: String::new(),
        })
        .expect_err("should reject plotting instructions");
        assert!(error.to_string().contains("forbidden phrase"));
    }

    #[test]
    fn answer_validation_allows_plain_analyst_takeaway() {
        validate_final_answer(&FinalAnswer {
            answer: "NSW1's daily average RRP was highest on 2026-03-12 at A$59.25/MWh and lowest on 2026-03-10 at A$41.54/MWh.".to_string(),
            note: String::new(),
        })
        .expect("valid answer");
    }
}
