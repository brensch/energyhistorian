#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import textwrap
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from xml.sax.saxutils import escape as xml_escape

import clickhouse_connect
import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import Image, Paragraph, Preformatted, SimpleDocTemplate, Spacer, Table, TableStyle


CLICKHOUSE_HOST = "127.0.0.1"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = "energyhistorian"
CLICKHOUSE_PASSWORD = "energyhistorian"
DEFAULT_MODEL = "gpt-5-mini"
DEFAULT_OUTPUT_ROOT = Path("reports/ask_nem")
OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses"
OPENAI_TIMEOUT_SECONDS = 60


FALLBACK_REGISTRY = [
    {
        "source_id": "aemo.nemweb",
        "object_name": "semantic.dispatch_price",
        "object_kind": "view",
        "description": "Regional 5-minute energy and FCAS price outcomes by settlement interval.",
        "grain": "one row per settlement interval per region",
        "time_column": "SETTLEMENTDATE",
        "dimensions": ["REGIONID"],
        "measures": ["RRP", "RAISE6SECRRP", "RAISE60SECRRP", "RAISE5MINRRP", "RAISEREGRRP", "LOWER6SECRRP", "LOWER60SECRRP", "LOWER5MINRRP", "LOWERREGRRP"],
        "join_keys": ["semantic.daily_region_dispatch on SETTLEMENTDATE, REGIONID"],
        "caveats": ["Operational price outcomes, not settlement amounts."],
        "question_tags": ["price", "volatility", "fcas", "region", "spread"],
    },
    {
        "source_id": "aemo.nemweb",
        "object_name": "semantic.daily_region_dispatch",
        "object_kind": "view",
        "description": "Regional dispatch outcomes, demand, interchange, available generation, and regional FCAS enablement by settlement interval.",
        "grain": "one row per settlement interval per region",
        "time_column": "SETTLEMENTDATE",
        "dimensions": ["REGIONID"],
        "measures": ["RRP", "TOTALDEMAND", "DEMANDFORECAST", "DISPATCHABLEGENERATION", "NETINTERCHANGE", "AVAILABLEGENERATION", "EXCESSGENERATION"],
        "join_keys": ["semantic.dispatch_price on SETTLEMENTDATE, REGIONID"],
        "caveats": ["Operational regional dispatch fact, not settlement-grade billing."],
        "question_tags": ["demand", "dispatch", "region", "imports", "fcas", "price"],
    },
    {
        "source_id": "aemo.nemweb",
        "object_name": "semantic.daily_unit_dispatch",
        "object_kind": "view",
        "description": "Unit-level dispatch targets, availability, ramp rates, and FCAS enablement by settlement interval.",
        "grain": "one row per settlement interval per DUID",
        "time_column": "SETTLEMENTDATE",
        "dimensions": ["DUID"],
        "measures": ["TOTALCLEARED", "INITIALMW", "AVAILABILITY", "RAMPUPRATE", "RAMPDOWNRATE", "RAISE6SEC", "RAISE60SEC", "RAISE5MIN", "RAISEREG", "LOWER6SEC", "LOWER60SEC", "LOWER5MIN", "LOWERREG"],
        "join_keys": ["semantic.unit_dimension on DUID"],
        "caveats": ["Dispatch targets are operational outcomes, not metered generation."],
        "question_tags": ["duid", "dispatch", "ramp", "battery", "fcas"],
    },
    {
        "source_id": "aemo.nemweb",
        "object_name": "semantic.dispatch_unit_solution",
        "object_kind": "view",
        "description": "Richer unit dispatch solution surface including UIGF, storage, and semi-scheduled fields by settlement interval.",
        "grain": "one row per settlement interval per DUID",
        "time_column": "SETTLEMENTDATE",
        "dimensions": ["DUID"],
        "measures": ["TOTALCLEARED", "UIGF", "ENERGY_STORAGE", "INITIAL_ENERGY_STORAGE"],
        "join_keys": ["semantic.unit_dimension on DUID"],
        "caveats": ["Use this when daily_unit_dispatch does not expose the required semi-scheduled or storage field."],
        "question_tags": ["curtailment", "semi-scheduled", "storage"],
    },
    {
        "source_id": "aemo.nemweb",
        "object_name": "semantic.actual_gen_duid",
        "object_kind": "view",
        "description": "Metered actual generation or energy readings by interval and DUID.",
        "grain": "one row per interval per DUID",
        "time_column": "INTERVAL_DATETIME",
        "dimensions": ["DUID"],
        "measures": ["MWH_READING"],
        "join_keys": ["semantic.unit_dimension on DUID"],
        "caveats": ["Best actual-generation surface for output and generation mix questions."],
        "question_tags": ["generation", "actual", "fuel", "mix"],
    },
    {
        "source_id": "aemo.nemweb",
        "object_name": "semantic.dispatch_interconnectorres",
        "object_kind": "view",
        "description": "Interconnector dispatch results including flow, limits, losses, and marginal value.",
        "grain": "one row per settlement interval per interconnector",
        "time_column": "SETTLEMENTDATE",
        "dimensions": ["INTERCONNECTORID"],
        "measures": ["MWFLOW", "METEREDMWFLOW", "MWLOSSES", "EXPORTLIMIT", "IMPORTLIMIT", "MARGINALVALUE"],
        "join_keys": ["semantic.dispatch_price by aligned settlement interval for spread analysis"],
        "caveats": ["Interconnector-region mapping is semantic knowledge rather than explicit table columns."],
        "question_tags": ["interconnector", "flow", "limit", "loss", "spread"],
    },
    {
        "source_id": "aemo.nemweb",
        "object_name": "semantic.dispatch_constraint",
        "object_kind": "view",
        "description": "Constraint outcomes by settlement interval and constraint identifier.",
        "grain": "one row per settlement interval per constraint",
        "time_column": "SETTLEMENTDATE",
        "dimensions": ["CONSTRAINTID"],
        "measures": ["RHS", "LHS", "MARGINALVALUE", "VIOLATIONDEGREE"],
        "join_keys": [],
        "caveats": ["Constraint type may need to be proxied from CONSTRAINTID unless a dedicated dimension is added."],
        "question_tags": ["constraint", "binding", "shadow price"],
    },
    {
        "source_id": "aemo.nemweb",
        "object_name": "semantic.bid_dayoffer",
        "object_kind": "view",
        "description": "Daily bid price bands and rebid explanations by DUID, participant, and bid type.",
        "grain": "one row per settlement day per DUID per participant per bid type",
        "time_column": "SETTLEMENTDATE",
        "dimensions": ["DUID", "PARTICIPANTID", "BIDTYPE"],
        "measures": ["VERSIONNO", "PRICEBAND1", "PRICEBAND5", "PRICEBAND10"],
        "join_keys": ["semantic.bid_peroffer on SETTLEMENTDATE, DUID, BIDTYPE", "semantic.unit_dimension on DUID"],
        "caveats": ["Day offers describe offered prices, not dispatched quantities."],
        "question_tags": ["bids", "rebid", "offers"],
    },
    {
        "source_id": "aemo.nemweb",
        "object_name": "semantic.bid_peroffer",
        "object_kind": "view",
        "description": "Interval bid availabilities by DUID and bid type.",
        "grain": "one row per interval per DUID per bid type",
        "time_column": "INTERVAL_DATETIME",
        "dimensions": ["DUID", "BIDTYPE"],
        "measures": ["MAXAVAIL", "BANDAVAIL1", "BANDAVAIL10"],
        "join_keys": ["semantic.bid_dayoffer on SETTLEMENTDATE, DUID, BIDTYPE"],
        "caveats": ["Band availability is an offer surface, not actual dispatch."],
        "question_tags": ["bids", "availability", "offers"],
    },
    {
        "source_id": "aemo.mmsdm",
        "object_name": "semantic.unit_dimension",
        "object_kind": "view",
        "description": "Current reconciled DUID dimension derived from MMSDM registration history.",
        "grain": "one row per DUID in current effective state",
        "time_column": "START_DATE",
        "dimensions": ["DUID", "REGIONID", "PARTICIPANTID", "EFFECTIVE_PARTICIPANTID", "STATIONID", "STATIONNAME", "FUEL_TYPE", "DISPATCHTYPE", "SCHEDULE_TYPE", "IS_STORAGE", "IS_BIDIRECTIONAL"],
        "measures": ["REGISTEREDCAPACITY_MW", "MAXCAPACITY_MW", "MAXSTORAGECAPACITY_MWH", "CO2E_EMISSIONS_FACTOR"],
        "join_keys": ["semantic.daily_unit_dispatch on DUID", "semantic.actual_gen_duid on DUID", "semantic.bid_dayoffer on DUID"],
        "caveats": ["Current-state dimension built from historised MMSDM registration sources."],
        "question_tags": ["duid", "participant", "station", "fuel", "capacity", "battery"],
    },
    {
        "source_id": "aemo.mmsdm",
        "object_name": "semantic.participant_registration_dudetailsummary",
        "object_kind": "view",
        "description": "Historised MMSDM DUID summary registration records.",
        "grain": "one row per effective-dated DUID summary record",
        "time_column": "START_DATE",
        "dimensions": ["DUID", "REGIONID", "PARTICIPANTID", "STATIONID", "DISPATCHTYPE", "SCHEDULE_TYPE"],
        "measures": ["TRANSMISSIONLOSSFACTOR", "DISTRIBUTIONLOSSFACTOR", "MAX_RAMP_RATE_UP", "MAX_RAMP_RATE_DOWN"],
        "join_keys": ["semantic.unit_dimension on DUID"],
        "caveats": ["Historised MMSDM registration summary surface."],
        "question_tags": ["registration", "history", "duid", "participant"],
    },
]


@dataclass
class Plan:
    status: str
    sql: str
    used_objects: list[str]
    data_description: str
    note: str
    chart_title: str
    confidence: str
    reason: str


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        os.environ.setdefault(key, value)
        if key.lower() == "openai_key":
            os.environ.setdefault("OPENAI_API_KEY", value)


def slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-") or "question"


def connect():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )


def query_dataframe(client, sql: str) -> pd.DataFrame:
    result = client.query(sql)
    return pd.DataFrame(result.result_rows, columns=result.column_names)


def load_registry(client) -> tuple[list[dict[str, Any]], str]:
    queries = [
        "SELECT * FROM semantic.semantic_model_registry ORDER BY object_name",
        "SELECT * FROM semantic.nemweb_model_registry UNION ALL SELECT * FROM semantic.mmsdm_model_registry ORDER BY object_name",
    ]
    for sql in queries:
        try:
            rows = query_dataframe(client, sql)
            if not rows.empty:
                return rows.to_dict(orient="records"), "clickhouse"
        except Exception:
            pass
    return FALLBACK_REGISTRY, "fallback"


def call_openai(model: str, system_prompt: str, user_prompt: str) -> str:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set")

    payload = {
        "model": model,
        "input": [
            {"role": "system", "content": [{"type": "input_text", "text": system_prompt}]},
            {"role": "user", "content": [{"type": "input_text", "text": user_prompt}]},
        ],
    }
    request = urllib.request.Request(
        OPENAI_RESPONSES_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=OPENAI_TIMEOUT_SECONDS) as response:
            body = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        details = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"OpenAI API error: HTTP {exc.code}: {details}") from exc

    if "output_text" in body and body["output_text"]:
        return body["output_text"]

    parts: list[str] = []
    for item in body.get("output", []):
        for content in item.get("content", []):
            if content.get("type") in {"output_text", "text"} and content.get("text"):
                parts.append(content["text"])
    if parts:
        return "\n".join(parts)
    raise RuntimeError(f"OpenAI response did not contain text output: {json.dumps(body)[:1200]}")


def extract_json(text: str) -> dict[str, Any]:
    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if not match:
            raise
        return json.loads(match.group(0))


def plan_question(question: str, registry: list[dict[str, Any]], model: str) -> Plan:
    system_prompt = (
        "You are a careful NEM analytics planner. "
        "You must decide whether the user's question can be answered from the provided semantic registry. "
        "If answerable, write one read-only ClickHouse SQL query using only semantic.* objects from the registry. "
        "If not answerable, return blocked with an empty SQL string. "
        "Prefer concise SQL with explicit grouping and limits. "
        "Use proxies only when the registry caveats imply they are acceptable, and say so in the note. "
        "Return JSON only."
    )
    user_prompt = json.dumps(
        {
            "question": question,
            "required_output_schema": {
                "status": "answerable|blocked",
                "sql": "string",
                "used_objects": ["semantic.object_name"],
                "data_description": "string",
                "note": "string",
                "chart_title": "string",
                "confidence": "high|medium|low",
                "reason": "string",
            },
            "semantic_registry": registry,
        },
        indent=2,
    )
    parsed = extract_json(call_openai(model, system_prompt, user_prompt))
    return Plan(
        status=str(parsed.get("status", "blocked")),
        sql=str(parsed.get("sql", "")).strip(),
        used_objects=[str(value) for value in parsed.get("used_objects", [])],
        data_description=str(parsed.get("data_description", "")).strip(),
        note=str(parsed.get("note", "")).strip(),
        chart_title=str(parsed.get("chart_title", "")).strip() or question,
        confidence=str(parsed.get("confidence", "low")).strip(),
        reason=str(parsed.get("reason", "")).strip(),
    )


def refine_answer(question: str, plan: Plan, df: pd.DataFrame, model: str) -> tuple[str, str]:
    preview = df.head(20).fillna("").astype(str).to_dict(orient="records")
    summary = {
        "row_count": int(len(df)),
        "columns": list(df.columns),
        "preview": preview,
    }
    system_prompt = (
        "You are a careful NEM analyst. "
        "Write a concise explanation of the query result and a short analyst note. "
        "Respect the provided caveats and do not claim settlement-grade accuracy if the plan says otherwise. "
        "Return JSON only."
    )
    user_prompt = json.dumps(
        {
            "question": question,
            "plan": {
                "data_description": plan.data_description,
                "note": plan.note,
                "confidence": plan.confidence,
                "reason": plan.reason,
                "used_objects": plan.used_objects,
            },
            "results_summary": summary,
            "required_output_schema": {
                "explanation": "string",
                "note": "string",
            },
        },
        indent=2,
    )
    parsed = extract_json(call_openai(model, system_prompt, user_prompt))
    explanation = str(parsed.get("explanation", "")).strip()
    note = str(parsed.get("note", "")).strip() or plan.note
    return explanation, note


def sanitize_sql(sql: str) -> str:
    return sql.strip().rstrip(";")


def validate_sql(sql: str, allowed_objects: set[str]) -> str:
    sql = sanitize_sql(sql)
    lowered = sql.lower()
    if not lowered.startswith(("select", "with")):
        raise ValueError("Only SELECT/WITH queries are allowed")
    forbidden = ["insert", "delete", "update", "drop", "truncate", "alter", "create", "attach", "detach", "grant", "revoke"]
    if any(re.search(rf"\b{keyword}\b", lowered) for keyword in forbidden):
        raise ValueError("SQL contains forbidden write or DDL keywords")
    if re.search(r"\braw_[a-z0-9_]+\b", lowered) or "system." in lowered:
        raise ValueError("SQL must stay within semantic.* objects")

    referenced = set(re.findall(r"semantic\.[a-zA-Z0-9_]+", sql))
    if not referenced:
        raise ValueError("SQL did not reference any semantic objects")
    unknown = sorted(referenced - allowed_objects)
    if unknown:
        raise ValueError(f"SQL referenced objects not in registry: {', '.join(unknown)}")
    return sql


def repair_plan(question: str, registry: list[dict[str, Any]], plan: Plan, error: str, model: str) -> Plan:
    system_prompt = (
        "You are repairing a failed ClickHouse SQL plan. "
        "Keep the same JSON schema as before. "
        "Only use semantic.* objects from the registry. "
        "If the question cannot be answered cleanly, return blocked."
    )
    user_prompt = json.dumps(
        {
            "question": question,
            "failing_plan": plan.__dict__,
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
                "reason": "string",
            },
        },
        indent=2,
    )
    parsed = extract_json(call_openai(model, system_prompt, user_prompt))
    return Plan(
        status=str(parsed.get("status", "blocked")),
        sql=str(parsed.get("sql", "")).strip(),
        used_objects=[str(value) for value in parsed.get("used_objects", [])],
        data_description=str(parsed.get("data_description", "")).strip(),
        note=str(parsed.get("note", "")).strip(),
        chart_title=str(parsed.get("chart_title", "")).strip() or question,
        confidence=str(parsed.get("confidence", "low")).strip(),
        reason=str(parsed.get("reason", "")).strip(),
    )


def build_chart(df: pd.DataFrame, title: str, out_path: Path, blocked: bool) -> None:
    plt.style.use("ggplot")
    fig, ax = plt.subplots(figsize=(10, 5.5))
    if blocked:
        ax.text(0.5, 0.5, "Blocked or not yet answerable", ha="center", va="center", fontsize=18)
        ax.axis("off")
    elif df.empty:
        ax.text(0.5, 0.5, "Query returned no rows", ha="center", va="center", fontsize=18)
        ax.axis("off")
    else:
        dt_cols = [c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c])]
        numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        category_cols = [c for c in df.columns if c not in dt_cols and c not in numeric_cols]
        if dt_cols and numeric_cols:
            x_col = dt_cols[0]
            y_cols = numeric_cols[: min(3, len(numeric_cols))]
            for y_col in y_cols:
                ax.plot(df[x_col], df[y_col], label=y_col)
            if len(y_cols) > 1:
                ax.legend()
            ax.set_xlabel(x_col)
            ax.set_ylabel(", ".join(y_cols))
        elif category_cols and numeric_cols:
            x_col = category_cols[0]
            y_col = numeric_cols[0]
            sub = df.head(20)
            ax.barh(sub[x_col].astype(str), sub[y_col], color="#1f77b4")
            ax.invert_yaxis()
            ax.set_xlabel(y_col)
            ax.set_ylabel(x_col)
        elif len(numeric_cols) >= 2:
            ax.scatter(df[numeric_cols[0]], df[numeric_cols[1]], alpha=0.75)
            ax.set_xlabel(numeric_cols[0])
            ax.set_ylabel(numeric_cols[1])
        else:
            ax.text(0.5, 0.5, "Table preview only", ha="center", va="center", fontsize=18)
            ax.axis("off")
    fig.suptitle(title, fontsize=12)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def pdf_text(value: str) -> str:
    return xml_escape(value or "")


def write_markdown(question: str, plan: Plan, explanation: str, target_dir: Path) -> None:
    md = (
        f"# {question}\n\n"
        f"- Status: `{plan.status}`\n"
        f"- Confidence: `{plan.confidence}`\n"
        f"- Raw results: `results.csv`\n\n"
        f"## Data\n\n{plan.data_description}\n\n"
        f"## Note\n\n{plan.note}\n\n"
        f"## Explanation\n\n{explanation}\n\n"
        f"## SQL\n\n```sql\n{plan.sql}\n```\n"
    )
    (target_dir / "report.md").write_text(md)


def write_pdf(question: str, plan: Plan, explanation: str, df: pd.DataFrame, target_dir: Path, chart_path: Path) -> None:
    doc = SimpleDocTemplate(str(target_dir / "report.pdf"), pagesize=A4, rightMargin=36, leftMargin=36, topMargin=36, bottomMargin=36)
    styles = getSampleStyleSheet()
    mono = ParagraphStyle("MonoSmall", parent=styles["Code"], fontName="Courier", fontSize=7.5, leading=9)
    elements = [
        Paragraph(pdf_text(question), styles["Title"]),
        Spacer(1, 8),
        Paragraph(f"Status: <b>{pdf_text(plan.status)}</b>", styles["BodyText"]),
        Paragraph(f"Confidence: <b>{pdf_text(plan.confidence)}</b>", styles["BodyText"]),
        Spacer(1, 8),
        Paragraph("Data", styles["Heading2"]),
        Paragraph(pdf_text(plan.data_description), styles["BodyText"]),
        Spacer(1, 8),
        Paragraph("Note", styles["Heading2"]),
        Paragraph(pdf_text(plan.note), styles["BodyText"]),
        Spacer(1, 8),
        Paragraph("Explanation", styles["Heading2"]),
        Paragraph(pdf_text(explanation), styles["BodyText"]),
        Spacer(1, 12),
    ]
    if chart_path.exists():
        elements.append(Image(str(chart_path), width=6.9 * inch, height=3.8 * inch))
        elements.append(Spacer(1, 12))
    preview = df.head(12).fillna("").astype(str)
    if not preview.empty:
        table = Table([list(preview.columns)] + preview.values.tolist(), repeatRows=1)
        table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#dce6f1")),
                    ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
                    ("FONTSIZE", (0, 0), (-1, -1), 7),
                    ("LEADING", (0, 0), (-1, -1), 8),
                ]
            )
        )
        elements.append(Paragraph("Result Preview", styles["Heading2"]))
        elements.append(table)
        elements.append(Spacer(1, 12))
    elements.append(Paragraph("SQL", styles["Heading2"]))
    elements.append(Preformatted(plan.sql or "-- blocked", mono))
    doc.build(elements)


def write_bundle(question: str, plan: Plan, explanation: str, df: pd.DataFrame, output_dir: Path, registry_source: str) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    chart_path = output_dir / "chart.png"
    build_chart(df, plan.chart_title or question, chart_path, blocked=plan.status != "answerable")
    write_markdown(question, plan, explanation, output_dir)
    write_pdf(question, plan, explanation, df, output_dir, chart_path)
    (output_dir / "query.sql").write_text(plan.sql or "-- blocked")
    df.to_csv(output_dir / "results.csv", index=False)
    (output_dir / "answer.json").write_text(
        json.dumps(
            {
                "question": question,
                "status": plan.status,
                "confidence": plan.confidence,
                "used_objects": plan.used_objects,
                "data_description": plan.data_description,
                "note": plan.note,
                "reason": plan.reason,
                "sql": plan.sql,
                "registry_source": registry_source,
                "generated_at": datetime.now(timezone.utc).isoformat(),
            },
            indent=2,
        )
    )


def run_query(client, question: str, registry: list[dict[str, Any]], model: str) -> tuple[Plan, pd.DataFrame]:
    allowed_objects = {entry["object_name"] for entry in registry}
    plan = plan_question(question, registry, model)
    if plan.status != "answerable" or not plan.sql:
        blocked = Plan(
            status="blocked",
            sql="",
            used_objects=plan.used_objects,
            data_description=plan.data_description or "The required semantic surface is not currently available.",
            note=plan.note or "The semantic layer does not currently support this question cleanly.",
            chart_title=plan.chart_title or question,
            confidence=plan.confidence or "low",
            reason=plan.reason,
        )
        return blocked, pd.DataFrame()

    last_error = None
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            sql = validate_sql(plan.sql, allowed_objects)
            client.query(f"EXPLAIN SYNTAX {sql}")
            df = query_dataframe(client, sql)
            if not df.empty:
                return plan, df
            last_error = "Query returned no rows"
        except Exception as exc:
            last_error = str(exc)
        if attempt < max_attempts - 1:
            plan = repair_plan(question, registry, plan, last_error, model)
            if plan.status != "answerable" or not plan.sql:
                break

    if last_error == "Query returned no rows":
        no_rows = Plan(
            status="blocked",
            sql=plan.sql,
            used_objects=plan.used_objects,
            data_description=plan.data_description or "The planner found a plausible semantic surface, but the resulting queries returned no rows.",
            note=plan.note or "The planner retried with alternative SQL, but the available semantic data still returned no rows for this question.",
            chart_title=plan.chart_title or question,
            confidence="low",
            reason=last_error,
        )
        return no_rows, pd.DataFrame()

    blocked = Plan(
        status="blocked",
        sql="",
        used_objects=plan.used_objects,
        data_description=plan.data_description or "The planned semantic query could not be validated or executed cleanly.",
        note=plan.note or "The planner could not produce a safe query for the current semantic layer.",
        chart_title=plan.chart_title or question,
        confidence="low",
        reason=last_error or plan.reason,
    )
    return blocked, pd.DataFrame()


def default_output_dir(question: str) -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return DEFAULT_OUTPUT_ROOT / f"{timestamp}-{slugify(question)[:80]}"


def main() -> int:
    load_dotenv(Path(".env"))

    parser = argparse.ArgumentParser(description="Answer an arbitrary NEM question using the semantic layer and OpenAI.")
    parser.add_argument("question", help="Natural-language question to answer")
    parser.add_argument("--model", default=os.getenv("OPENAI_MODEL", DEFAULT_MODEL))
    parser.add_argument("--output", type=Path, default=None, help="Output directory for the report bundle")
    args = parser.parse_args()

    client = connect()
    registry, registry_source = load_registry(client)
    output_dir = args.output or default_output_dir(args.question)

    plan, df = run_query(client, args.question, registry, args.model)
    if plan.status == "answerable":
        explanation, refined_note = refine_answer(args.question, plan, df, args.model)
        if refined_note:
            plan.note = refined_note
    else:
        explanation = plan.reason or "The question could not be answered from the current semantic layer."

    write_bundle(args.question, plan, explanation, df, output_dir, registry_source)
    print(
        json.dumps(
            {
                "output_dir": str(output_dir),
                "status": plan.status,
                "confidence": plan.confidence,
                "registry_source": registry_source,
                "rows": int(len(df)),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(json.dumps({"error": str(exc)}, indent=2), file=sys.stderr)
        raise SystemExit(1)
