#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import re
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import clickhouse_connect
import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (
    Image,
    Paragraph,
    Preformatted,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)


QUESTION_FILE = Path("nem-questions.txt")
OUTPUT_ROOT = Path("reports/nem_questions")
CLICKHOUSE_HOST = "127.0.0.1"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = "energyhistorian"
CLICKHOUSE_PASSWORD = "energyhistorian"

REGION_DISPATCH_CTE = """
region_dispatch AS (
    SELECT
        SETTLEMENTDATE,
        REGIONID,
        avg(RRP) AS RRP,
        avg(TOTALDEMAND) AS TOTALDEMAND,
        avg(DEMANDFORECAST) AS DEMANDFORECAST,
        avg(DISPATCHABLEGENERATION) AS DISPATCHABLEGENERATION,
        avg(NETINTERCHANGE) AS NETINTERCHANGE,
        avg(AVAILABLEGENERATION) AS AVAILABLEGENERATION,
        avg(EXCESSGENERATION) AS EXCESSGENERATION,
        avg(RAISE6SECDISPATCH) AS RAISE6SECDISPATCH,
        avg(RAISE60SECDISPATCH) AS RAISE60SECDISPATCH,
        avg(RAISE5MINDISPATCH) AS RAISE5MINDISPATCH,
        avg(RAISEREGLOCALDISPATCH) AS RAISEREGLOCALDISPATCH,
        avg(LOWER6SECDISPATCH) AS LOWER6SECDISPATCH,
        avg(LOWER60SECDISPATCH) AS LOWER60SECDISPATCH,
        avg(LOWER5MINDISPATCH) AS LOWER5MINDISPATCH,
        avg(LOWERREGLOCALDISPATCH) AS LOWERREGLOCALDISPATCH
    FROM semantic.daily_region_dispatch
    GROUP BY SETTLEMENTDATE, REGIONID
)
"""

PRICE_DISPATCH_CTE = """
price_dispatch AS (
    SELECT
        SETTLEMENTDATE,
        REGIONID,
        avg(RRP) AS RRP,
        avg(RAISE6SECRRP) AS RAISE6SECRRP,
        avg(RAISE60SECRRP) AS RAISE60SECRRP,
        avg(RAISE5MINRRP) AS RAISE5MINRRP,
        avg(RAISEREGRRP) AS RAISEREGRRP,
        avg(RAISE1SECRRP) AS RAISE1SECRRP,
        avg(LOWER6SECRRP) AS LOWER6SECRRP,
        avg(LOWER60SECRRP) AS LOWER60SECRRP,
        avg(LOWER5MINRRP) AS LOWER5MINRRP,
        avg(LOWERREGRRP) AS LOWERREGRRP,
        avg(LOWER1SECRRP) AS LOWER1SECRRP
    FROM semantic.dispatch_price
    GROUP BY SETTLEMENTDATE, REGIONID
)
"""

UNIT_DISPATCH_CTE = """
unit_dispatch AS (
    SELECT
        SETTLEMENTDATE,
        DUID,
        avg(TOTALCLEARED) AS TOTALCLEARED,
        avg(INITIALMW) AS INITIALMW,
        avg(AVAILABILITY) AS AVAILABILITY,
        avg(RAMPUPRATE) AS RAMPUPRATE,
        avg(RAMPDOWNRATE) AS RAMPDOWNRATE,
        avg(RAISE6SEC + RAISE60SEC + RAISE5MIN + RAISEREG) AS FCAS_RAISE_TOTAL,
        avg(LOWER6SEC + LOWER60SEC + LOWER5MIN + LOWERREG) AS FCAS_LOWER_TOTAL
    FROM semantic.daily_unit_dispatch
    GROUP BY SETTLEMENTDATE, DUID
)
"""

UNIT_SOLUTION_CTE = """
unit_solution AS (
    SELECT
        SETTLEMENTDATE,
        DUID,
        avg(TOTALCLEARED) AS TOTALCLEARED,
        avg(UIGF) AS UIGF
    FROM semantic.dispatch_unit_solution
    GROUP BY SETTLEMENTDATE, DUID
)
"""

ACTUAL_GEN_CTE = """
actual_gen AS (
    SELECT
        INTERVAL_DATETIME,
        DUID,
        avg(MWH_READING) AS MWH_READING
    FROM semantic.actual_gen_duid
    GROUP BY INTERVAL_DATETIME, DUID
)
"""

SCADA_CTE = """
unit_scada AS (
    SELECT
        SETTLEMENTDATE,
        DUID,
        avg(SCADAVALUE) AS SCADAVALUE
    FROM semantic.dispatch_unit_scada
    GROUP BY SETTLEMENTDATE, DUID
)
"""

INTERCONNECTOR_CTE = """
interconnector_dispatch AS (
    SELECT
        SETTLEMENTDATE,
        INTERCONNECTORID,
        avg(MWFLOW) AS MWFLOW,
        avg(METEREDMWFLOW) AS METEREDMWFLOW,
        avg(MWLOSSES) AS MWLOSSES,
        avg(EXPORTLIMIT) AS EXPORTLIMIT,
        avg(IMPORTLIMIT) AS IMPORTLIMIT,
        avg(MARGINALVALUE) AS MARGINALVALUE
    FROM semantic.dispatch_interconnectorres
    GROUP BY SETTLEMENTDATE, INTERCONNECTORID
)
"""

CONSTRAINT_CTE = """
constraint_dispatch AS (
    SELECT
        SETTLEMENTDATE,
        CONSTRAINTID,
        avg(RHS) AS RHS,
        avg(LHS) AS LHS,
        avg(MARGINALVALUE) AS MARGINALVALUE,
        avg(VIOLATIONDEGREE) AS VIOLATIONDEGREE
    FROM semantic.dispatch_constraint
    GROUP BY SETTLEMENTDATE, CONSTRAINTID
)
"""

PREDISPATCH_PRICE_CTE = """
predispatch_price AS (
    SELECT
        DATETIME,
        REGIONID,
        avg(RRP) AS RRP
    FROM semantic.predispatch_region_prices
    GROUP BY DATETIME, REGIONID
)
"""

PREDISPATCH_SOLUTION_CTE = """
predispatch_solution AS (
    SELECT
        DATETIME,
        REGIONID,
        avg(TOTALDEMAND) AS TOTALDEMAND,
        avg(DEMANDFORECAST) AS DEMANDFORECAST
    FROM semantic.predispatch_region_solution
    GROUP BY DATETIME, REGIONID
)
"""

P5_REGION_CTE = """
p5_region AS (
    SELECT
        INTERVAL_DATETIME,
        RUN_DATETIME,
        REGIONID,
        avg(RRP) AS RRP,
        avg(TOTALDEMAND) AS TOTALDEMAND,
        avg(DEMANDFORECAST) AS DEMANDFORECAST
    FROM semantic.p5min_regionsolution
    GROUP BY INTERVAL_DATETIME, RUN_DATETIME, REGIONID
)
"""

BID_DAY_CTE = """
bid_day AS (
    SELECT
        SETTLEMENTDATE,
        DUID,
        PARTICIPANTID,
        BIDTYPE,
        avg(VERSIONNO) AS VERSIONNO,
        avg(PRICEBAND1) AS PRICEBAND1,
        avg(PRICEBAND2) AS PRICEBAND2,
        avg(PRICEBAND3) AS PRICEBAND3,
        avg(PRICEBAND4) AS PRICEBAND4,
        avg(PRICEBAND5) AS PRICEBAND5,
        avg(PRICEBAND6) AS PRICEBAND6,
        avg(PRICEBAND7) AS PRICEBAND7,
        avg(PRICEBAND8) AS PRICEBAND8,
        avg(PRICEBAND9) AS PRICEBAND9,
        avg(PRICEBAND10) AS PRICEBAND10,
        anyLast(REBIDEXPLANATION) AS REBIDEXPLANATION
    FROM semantic.bid_dayoffer
    GROUP BY SETTLEMENTDATE, DUID, PARTICIPANTID, BIDTYPE
)
"""

BID_PER_CTE = """
bid_period AS (
    SELECT
        INTERVAL_DATETIME,
        SETTLEMENTDATE,
        DUID,
        BIDTYPE,
        avg(MAXAVAIL) AS MAXAVAIL,
        avg(BANDAVAIL1) AS BANDAVAIL1,
        avg(BANDAVAIL2) AS BANDAVAIL2,
        avg(BANDAVAIL3) AS BANDAVAIL3,
        avg(BANDAVAIL4) AS BANDAVAIL4,
        avg(BANDAVAIL5) AS BANDAVAIL5,
        avg(BANDAVAIL6) AS BANDAVAIL6,
        avg(BANDAVAIL7) AS BANDAVAIL7,
        avg(BANDAVAIL8) AS BANDAVAIL8,
        avg(BANDAVAIL9) AS BANDAVAIL9,
        avg(BANDAVAIL10) AS BANDAVAIL10
    FROM semantic.bid_peroffer
    GROUP BY INTERVAL_DATETIME, SETTLEMENTDATE, DUID, BIDTYPE
)
"""

UNIT_DIM_CTE = """
unit_dim AS (
    SELECT
        DUID,
        REGIONID,
        EFFECTIVE_PARTICIPANTID,
        PARTICIPANTID,
        STATIONID,
        STATIONNAME,
        FUEL_TYPE,
        ENERGY_SOURCE,
        REGISTEREDCAPACITY_MW,
        MAXCAPACITY_MW,
        MAXSTORAGECAPACITY_MWH,
        CO2E_EMISSIONS_FACTOR,
        IS_STORAGE,
        IS_BIDIRECTIONAL,
        DISPATCHTYPE,
        SCHEDULE_TYPE
    FROM semantic.unit_dimension
)
"""

INTERCONNECTOR_REGION_CASE = """
CASE
    WHEN INTERCONNECTORID IN ('NSW1-QLD1', 'N-Q-MNSP1') THEN 'NSW1-QLD1'
    WHEN INTERCONNECTORID = 'VIC1-NSW1' THEN 'VIC1-NSW1'
    WHEN INTERCONNECTORID IN ('V-SA', 'V-S-MNSP1') THEN 'VIC1-SA1'
    WHEN INTERCONNECTORID = 'T-V-MNSP1' THEN 'TAS1-VIC1'
    ELSE INTERCONNECTORID
END
"""

RENEWABLE_CASE = """
CASE
    WHEN lower(FUEL_TYPE) IN (
        'solar', 'wind', 'hydro', 'battery storage', 'bagasse',
        'biomass and industrial materials', 'other biofuels',
        'primary solid biomass fuels', 'landfill biogas methane'
    ) THEN 'Renewables'
    ELSE 'Fossil and other'
END
"""

FOSSIL_CASE = """
CASE
    WHEN lower(FUEL_TYPE) IN (
        'black coal', 'brown coal', 'natural gas (pipeline)',
        'coal seam methane', 'coal seam methane', 'coal seam methane',
        'coal mine waste gas', 'diesel oil', 'ethane',
        'kerosene - non aviation', 'other solid fossil fuels'
    ) THEN 'Fossil'
    ELSE 'Other'
END
"""


@dataclass
class QuerySpec:
    question: str
    slug: str
    status: str
    sql: str
    note: str
    chart_title: str
    data_description: str = ""


def slugify(value: str) -> str:
    value = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return value or "question"


def with_ctes(*ctes: str) -> str:
    return "WITH\n" + ",\n".join(ctes)


def diagnostic_sql(reason: str) -> str:
    safe_reason = reason.replace("'", "''")
    return f"SELECT 'blocked' AS status, '{safe_reason}' AS reason"


def available_window_note() -> str:
    return (
        "Results are generated from the current semantic layer in ClickHouse. "
        "If the warehouse does not yet contain full market history, the answer reflects "
        "the available window rather than authoritative all-time history."
    )


def external_dataset_note(reason: str) -> str:
    return (
        f"{reason} {available_window_note()}"
    )


DATA_SOURCE_DESCRIPTIONS = [
    (
        "region_dispatch",
        "`semantic.daily_region_dispatch`, reconciled to one row per settlement interval and region, "
        "providing regional demand, dispatch, interchange, available generation, excess generation, "
        "and regional FCAS enablement fields.",
    ),
    (
        "price_dispatch",
        "`semantic.dispatch_price`, reconciled to one row per settlement interval and region, "
        "providing 5-minute energy and FCAS regional price outcomes.",
    ),
    (
        "unit_dispatch",
        "`semantic.daily_unit_dispatch`, reconciled to one row per settlement interval and DUID, "
        "providing dispatch targets, initial MW, availability, ramp rates, and FCAS enablement.",
    ),
    (
        "unit_solution",
        "`semantic.dispatch_unit_solution`, reconciled to one row per settlement interval and DUID, "
        "providing the richer dispatch-solution surface including UIGF, storage fields, and semi-scheduled details.",
    ),
    (
        "actual_gen",
        "`semantic.actual_gen_duid`, reconciled to one row per interval and DUID, "
        "representing metered energy readings used as actual generation.",
    ),
    (
        "unit_scada",
        "`semantic.dispatch_unit_scada`, reconciled to one row per settlement interval and DUID, "
        "representing SCADA telemetry values.",
    ),
    (
        "interconnector_dispatch",
        "`semantic.dispatch_interconnectorres`, reconciled to one row per settlement interval and interconnector, "
        "providing flow, metered flow, losses, limits, and marginal value.",
    ),
    (
        "constraint_dispatch",
        "`semantic.dispatch_constraint`, reconciled to one row per settlement interval and constraint, "
        "providing RHS, LHS, marginal value, and violation degree.",
    ),
    (
        "predispatch_price",
        "`semantic.predispatch_region_prices`, reconciled to one row per forecast interval and region, "
        "providing predispatch regional price forecasts.",
    ),
    (
        "predispatch_solution",
        "`semantic.predispatch_region_solution`, reconciled to one row per forecast interval and region, "
        "providing predispatch demand and related regional solution fields.",
    ),
    (
        "p5_region",
        "`semantic.p5min_regionsolution`, reconciled to one row per target interval, run time, and region, "
        "providing 5-minute ahead forecast and solution outputs.",
    ),
    (
        "bid_day",
        "`semantic.bid_dayoffer`, reconciled to one row per settlement day, DUID, participant, and bid type, "
        "providing offered price bands and rebid explanations.",
    ),
    (
        "bid_period",
        "`semantic.bid_peroffer`, reconciled to one row per interval, DUID, and bid type, "
        "providing offered band availabilities and max availability.",
    ),
    (
        "unit_dim",
        "`semantic.unit_dimension`, the current reconciled DUID dimension derived from MMSDM registration history, "
        "providing region, participant, station, fuel, registered capacity, storage flags, dispatch type, and emissions factor.",
    ),
    (
        "semantic.participant_registration_dudetail",
        "`semantic.participant_registration_dudetail`, a historical MMSDM registration view with effective-dated DUID registration details.",
    ),
    (
        "semantic.participant_registration_dudetailsummary",
        "`semantic.participant_registration_dudetailsummary`, a historical MMSDM summary view for DUID registration periods and start dates.",
    ),
    (
        "semantic.marginal_loss_factors",
        "`semantic.marginal_loss_factors`, a NEMWEB-derived reference view containing transmission loss factors by DUID and connection point.",
    ),
]


def infer_data_description(sql: str, status: str) -> str:
    if status == "blocked":
        return (
            "No production semantic fact or dimension could be queried for this report. "
            "The SQL bundle is a diagnostic placeholder because the required dataset or modeled surface is not yet available in ClickHouse."
        )

    matched = []
    for token, description in DATA_SOURCE_DESCRIPTIONS:
        if token in sql and description not in matched:
            matched.append(description)

    if not matched:
        return (
            "This report is derived from the current `semantic.*` layer in ClickHouse, "
            "but the data source could not be classified into a more specific semantic surface automatically."
        )

    prefix = (
        "This report is derived from the following semantic surfaces in ClickHouse: "
        if len(matched) > 1
        else "This report is derived from the following semantic surface in ClickHouse: "
    )
    return prefix + " ".join(matched)


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


def infer_question(question: str) -> QuerySpec:
    q = question.strip()
    slug = slugify(q)
    note = available_window_note()

    if q == "What was the average spot price in each NEM region over the last 7 days?":
        sql = f"""{with_ctes(REGION_DISPATCH_CTE)}
SELECT REGIONID, round(avg(RRP), 2) AS avg_spot_price_mwh
FROM region_dispatch
WHERE SETTLEMENTDATE >= now() - INTERVAL 7 DAY
GROUP BY REGIONID
ORDER BY avg_spot_price_mwh DESC"""
        return QuerySpec(q, slug, "ready", sql, note, "Average spot price by region")

    if q == "Which region had the highest average spot price this week?":
        sql = f"""{with_ctes(REGION_DISPATCH_CTE)}
SELECT REGIONID, round(avg(RRP), 2) AS avg_spot_price_mwh
FROM region_dispatch
WHERE SETTLEMENTDATE >= date_trunc('week', now())
GROUP BY REGIONID
ORDER BY avg_spot_price_mwh DESC
LIMIT 1"""
        return QuerySpec(q, slug, "ready", sql, note, "Highest average spot price region")

    if q == "How volatile were 5-minute prices in each region over the past week?":
        sql = f"""{with_ctes(PRICE_DISPATCH_CTE)}
SELECT REGIONID, round(stddevPop(RRP), 2) AS price_volatility
FROM price_dispatch
WHERE SETTLEMENTDATE >= now() - INTERVAL 7 DAY
GROUP BY REGIONID
ORDER BY price_volatility DESC"""
        return QuerySpec(q, slug, "ready", sql, note, "Spot price volatility by region")

    if q == "What was the maximum spot price spike in each region in the last 30 days and when did it happen?":
        sql = f"""{with_ctes(PRICE_DISPATCH_CTE)}
SELECT
    REGIONID,
    max(RRP) AS max_spot_price_mwh,
    argMax(SETTLEMENTDATE, RRP) AS occurred_at
FROM price_dispatch
WHERE SETTLEMENTDATE >= now() - INTERVAL 30 DAY
GROUP BY REGIONID
ORDER BY max_spot_price_mwh DESC"""
        return QuerySpec(q, slug, "ready", sql, note, "Maximum price spike by region")

    if q == "How many intervals had negative spot prices in each region this month?":
        sql = f"""{with_ctes(PRICE_DISPATCH_CTE)}
SELECT REGIONID, countIf(RRP < 0) AS negative_intervals
FROM price_dispatch
WHERE toStartOfMonth(SETTLEMENTDATE) = toStartOfMonth(now())
GROUP BY REGIONID
ORDER BY negative_intervals DESC"""
        return QuerySpec(q, slug, "ready", sql, note, "Negative-price interval counts")

    if q == "What was the daily regional demand (MWh) over the last 7 days?":
        sql = f"""{with_ctes(REGION_DISPATCH_CTE)}
SELECT
    toDate(SETTLEMENTDATE) AS trading_day,
    REGIONID,
    round(sum(TOTALDEMAND / 12.0), 2) AS demand_mwh
FROM region_dispatch
WHERE SETTLEMENTDATE >= now() - INTERVAL 7 DAY
GROUP BY trading_day, REGIONID
ORDER BY trading_day, REGIONID"""
        return QuerySpec(q, slug, "ready", sql, note, "Daily regional demand")

    if q == "Which region had the biggest demand forecast error each day in the last 30 days?":
        sql = f"""{with_ctes(
            REGION_DISPATCH_CTE,
            """
day_region_error AS (
    SELECT
        toDate(SETTLEMENTDATE) AS trading_day,
        REGIONID,
        sum(abs(TOTALDEMAND - DEMANDFORECAST) / 12.0) AS abs_error_mwh
    FROM region_dispatch
    WHERE SETTLEMENTDATE >= now() - INTERVAL 30 DAY
    GROUP BY trading_day, REGIONID
)
"""
        )}
SELECT
    trading_day,
    REGIONID AS region_with_biggest_error,
    abs_error_mwh
FROM day_region_error
ORDER BY trading_day, abs_error_mwh DESC
LIMIT 1 BY trading_day"""
        return QuerySpec(q, slug, "ready", sql, note, "Largest daily demand forecast error")

    if q == "What was the peak demand in each region this week and when did it occur?":
        sql = f"""{with_ctes(REGION_DISPATCH_CTE)}
SELECT
    REGIONID,
    max(TOTALDEMAND) AS peak_demand_mw,
    argMax(SETTLEMENTDATE, TOTALDEMAND) AS occurred_at
FROM region_dispatch
WHERE SETTLEMENTDATE >= date_trunc('week', now())
GROUP BY REGIONID
ORDER BY peak_demand_mw DESC"""
        return QuerySpec(q, slug, "ready", sql, note, "Peak demand by region")

    if q == "How does weekday demand compare to weekend demand by region?":
        sql = f"""{with_ctes(REGION_DISPATCH_CTE)}
SELECT
    REGIONID,
    if(toDayOfWeek(SETTLEMENTDATE) IN (6, 7), 'Weekend', 'Weekday') AS day_type,
    round(avg(TOTALDEMAND), 2) AS avg_demand_mw
FROM region_dispatch
GROUP BY REGIONID, day_type
ORDER BY REGIONID, day_type"""
        return QuerySpec(q, slug, "ready", sql, note, "Weekday versus weekend demand")

    if q == "What is the average demand shape by hour of day for each region?":
        sql = f"""{with_ctes(REGION_DISPATCH_CTE)}
SELECT
    toHour(SETTLEMENTDATE) AS hour_of_day,
    REGIONID,
    round(avg(TOTALDEMAND), 2) AS avg_demand_mw
FROM region_dispatch
GROUP BY hour_of_day, REGIONID
ORDER BY hour_of_day, REGIONID"""
        return QuerySpec(q, slug, "ready", sql, note, "Average demand shape by hour")

    if q == "Which DUIDs produced the most energy (MWh) yesterday?":
        sql = f"""{with_ctes(ACTUAL_GEN_CTE, UNIT_DIM_CTE)}
SELECT
    g.DUID,
    any(u.REGIONID) AS REGIONID,
    any(u.FUEL_TYPE) AS FUEL_TYPE,
    round(sum(g.MWH_READING), 2) AS energy_mwh
FROM actual_gen g
LEFT JOIN unit_dim u ON u.DUID = g.DUID
WHERE toDate(g.INTERVAL_DATETIME) = yesterday()
GROUP BY g.DUID
ORDER BY energy_mwh DESC
LIMIT 20"""
        return QuerySpec(q, slug, "ready", sql, note, "Top energy-producing DUIDs yesterday")

    if q == "Which DUIDs had the highest peak dispatch target in the last 24 hours?":
        sql = f"""{with_ctes(UNIT_DISPATCH_CTE, UNIT_DIM_CTE)}
SELECT
    d.DUID,
    any(u.REGIONID) AS REGIONID,
    any(u.FUEL_TYPE) AS FUEL_TYPE,
    max(d.TOTALCLEARED) AS peak_dispatch_target_mw
FROM unit_dispatch d
LEFT JOIN unit_dim u ON u.DUID = d.DUID
WHERE d.SETTLEMENTDATE >= now() - INTERVAL 24 HOUR
GROUP BY d.DUID
ORDER BY peak_dispatch_target_mw DESC
LIMIT 20"""
        return QuerySpec(q, slug, "ready", sql, note, "Peak dispatch targets")

    if q == "What was the SCADA output profile of the top 10 generators over the last day?":
        sql = f"""{with_ctes(
            SCADA_CTE,
            UNIT_DIM_CTE,
            """
top_generators AS (
    SELECT DUID
    FROM (
        SELECT DUID, avg(SCADAVALUE) AS avg_scada
        FROM unit_scada
        WHERE SETTLEMENTDATE >= now() - INTERVAL 1 DAY
        GROUP BY DUID
        ORDER BY avg_scada DESC
        LIMIT 10
    )
)
"""
        )}
SELECT
    s.SETTLEMENTDATE,
    s.DUID,
    any(u.FUEL_TYPE) AS FUEL_TYPE,
    avg(s.SCADAVALUE) AS scada_mw
FROM unit_scada s
LEFT JOIN unit_dim u ON u.DUID = s.DUID
WHERE s.SETTLEMENTDATE >= now() - INTERVAL 1 DAY
  AND s.DUID IN (SELECT DUID FROM top_generators)
GROUP BY s.SETTLEMENTDATE, s.DUID
ORDER BY s.SETTLEMENTDATE, s.DUID"""
        return QuerySpec(q, slug, "ready", sql, note, "SCADA profiles for top generators")

    if q == "Which generators were dispatched above 90% of their registered capacity?":
        sql = f"""{with_ctes(UNIT_DISPATCH_CTE, UNIT_DIM_CTE)}
SELECT
    d.DUID,
    any(u.REGIONID) AS REGIONID,
    any(u.FUEL_TYPE) AS FUEL_TYPE,
    any(u.REGISTEREDCAPACITY_MW) AS registered_capacity_mw,
    max(d.TOTALCLEARED) AS max_dispatch_mw,
    round(max(d.TOTALCLEARED) / nullIf(any(u.REGISTEREDCAPACITY_MW), 0), 3) AS max_capacity_factor
FROM unit_dispatch d
INNER JOIN unit_dim u ON u.DUID = d.DUID
WHERE coalesce(u.DISPATCHTYPE, '') = 'GENERATOR'
GROUP BY d.DUID
HAVING max_dispatch_mw >= 0.9 * registered_capacity_mw
ORDER BY max_capacity_factor DESC
LIMIT 50"""
        return QuerySpec(q, slug, "ready", sql, note, "Generators above 90% of registered capacity")

    if q == "Which generators had the largest ramp rates in the last 24 hours?":
        sql = f"""{with_ctes(UNIT_DISPATCH_CTE, UNIT_DIM_CTE)}
SELECT
    DUID,
    any(REGIONID) AS REGIONID,
    any(FUEL_TYPE) AS FUEL_TYPE,
    max(abs(delta_mw_per_interval) * 12.0) AS max_ramp_rate_mw_per_hour
FROM (
    SELECT
        d.SETTLEMENTDATE,
        d.DUID,
        u.REGIONID,
        u.FUEL_TYPE,
        d.TOTALCLEARED - lagInFrame(d.TOTALCLEARED) OVER (
            PARTITION BY d.DUID ORDER BY d.SETTLEMENTDATE
        ) AS delta_mw_per_interval
    FROM unit_dispatch d
    INNER JOIN unit_dim u ON u.DUID = d.DUID
    WHERE d.SETTLEMENTDATE >= now() - INTERVAL 24 HOUR
      AND coalesce(u.DISPATCHTYPE, '') = 'GENERATOR'
)
GROUP BY DUID
ORDER BY max_ramp_rate_mw_per_hour DESC
LIMIT 20"""
        return QuerySpec(q, slug, "ready", sql, note, "Largest generator ramp rates")

    if q == "How much energy did each fuel type produce by region over the last 7 days?":
        sql = f"""{with_ctes(ACTUAL_GEN_CTE, UNIT_DIM_CTE)}
SELECT
    coalesce(u.REGIONID, 'UNKNOWN') AS REGIONID,
    coalesce(u.FUEL_TYPE, 'UNKNOWN') AS FUEL_TYPE,
    round(sum(g.MWH_READING), 2) AS energy_mwh
FROM actual_gen g
LEFT JOIN unit_dim u ON u.DUID = g.DUID
WHERE g.INTERVAL_DATETIME >= now() - INTERVAL 7 DAY
GROUP BY REGIONID, FUEL_TYPE
ORDER BY REGIONID, energy_mwh DESC"""
        return QuerySpec(q, slug, "ready", sql, note, "Energy by fuel type and region")

    if q == "What share of generation came from renewables vs fossil fuels by region?":
        sql = f"""{with_ctes(
            ACTUAL_GEN_CTE,
            UNIT_DIM_CTE,
            f"""
base AS (
    SELECT
        coalesce(u.REGIONID, 'UNKNOWN') AS REGIONID,
        {RENEWABLE_CASE} AS generation_group,
        g.MWH_READING AS energy_mwh
    FROM actual_gen g
    LEFT JOIN unit_dim u ON u.DUID = g.DUID
    WHERE g.INTERVAL_DATETIME >= now() - INTERVAL 7 DAY
)
"""
        )}
SELECT
    REGIONID,
    generation_group,
    round(energy_mwh, 2) AS energy_mwh,
    round(energy_mwh / sum(energy_mwh) OVER (PARTITION BY REGIONID), 4) AS share_of_region
FROM (
    SELECT
        REGIONID,
        generation_group,
        sum(energy_mwh) AS energy_mwh
    FROM base
    GROUP BY REGIONID, generation_group
)
ORDER BY REGIONID, share_of_region DESC"""
        return QuerySpec(q, slug, "ready", sql, note, "Renewables versus fossil generation share")

    if q == "What was the solar generation curve yesterday by region?":
        sql = f"""{with_ctes(ACTUAL_GEN_CTE, UNIT_DIM_CTE)}
SELECT
    toStartOfHour(g.INTERVAL_DATETIME) AS hour_start,
    u.REGIONID,
    round(sum(g.MWH_READING), 2) AS solar_mwh
FROM actual_gen g
INNER JOIN unit_dim u ON u.DUID = g.DUID
WHERE toDate(g.INTERVAL_DATETIME) = yesterday()
  AND lower(u.FUEL_TYPE) = 'solar'
GROUP BY hour_start, u.REGIONID
ORDER BY hour_start, u.REGIONID"""
        return QuerySpec(q, slug, "ready", sql, note, "Solar generation curve")

    if q == "What was the wind generation curve yesterday by region?":
        sql = f"""{with_ctes(ACTUAL_GEN_CTE, UNIT_DIM_CTE)}
SELECT
    toStartOfHour(g.INTERVAL_DATETIME) AS hour_start,
    u.REGIONID,
    round(sum(g.MWH_READING), 2) AS wind_mwh
FROM actual_gen g
INNER JOIN unit_dim u ON u.DUID = g.DUID
WHERE toDate(g.INTERVAL_DATETIME) = yesterday()
  AND lower(u.FUEL_TYPE) = 'wind'
GROUP BY hour_start, u.REGIONID
ORDER BY hour_start, u.REGIONID"""
        return QuerySpec(q, slug, "ready", sql, note, "Wind generation curve")

    if q == "How much coal generation has dropped month-over-month by region?":
        sql = f"""{with_ctes(
            ACTUAL_GEN_CTE,
            UNIT_DIM_CTE,
            """
monthly AS (
    SELECT
        toStartOfMonth(g.INTERVAL_DATETIME) AS month_start,
        u.REGIONID AS REGIONID,
        round(sum(g.MWH_READING), 2) AS coal_mwh
    FROM actual_gen g
    INNER JOIN unit_dim u ON u.DUID = g.DUID
    WHERE lower(u.FUEL_TYPE) IN ('black coal', 'brown coal')
    GROUP BY month_start, REGIONID
)
"""
        )}
SELECT
    month_start,
    REGIONID,
    coal_mwh,
    coal_mwh - lagInFrame(coal_mwh) OVER (PARTITION BY REGIONID ORDER BY month_start) AS change_vs_prior_month_mwh
FROM monthly
ORDER BY month_start, REGIONID"""
        return QuerySpec(q, slug, "limited", sql, note, "Coal generation month-over-month")

    if q == "How much energy did batteries send out (discharge) in each region over the last 7 days?":
        sql = f"""{with_ctes(UNIT_DISPATCH_CTE, UNIT_DIM_CTE)}
SELECT
    u.REGIONID,
    round(sum(greatest(d.TOTALCLEARED, 0) / 12.0), 2) AS discharge_mwh
FROM unit_dispatch d
INNER JOIN unit_dim u ON u.DUID = d.DUID
WHERE d.SETTLEMENTDATE >= now() - INTERVAL 7 DAY
  AND u.IS_STORAGE = 1
GROUP BY u.REGIONID
ORDER BY discharge_mwh DESC"""
        return QuerySpec(q, slug, "proxy", sql, note, "Battery discharge energy by region")

    if q == "How much energy did batteries consume (charge) in each region over the last 7 days?":
        sql = f"""{with_ctes(UNIT_DISPATCH_CTE, UNIT_DIM_CTE)}
SELECT
    u.REGIONID,
    round(sum(abs(least(d.TOTALCLEARED, 0)) / 12.0), 2) AS charge_mwh
FROM unit_dispatch d
INNER JOIN unit_dim u ON u.DUID = d.DUID
WHERE d.SETTLEMENTDATE >= now() - INTERVAL 7 DAY
  AND u.IS_STORAGE = 1
GROUP BY u.REGIONID
ORDER BY charge_mwh DESC"""
        return QuerySpec(q, slug, "proxy", sql, note, "Battery charging energy by region")

    if q == "What was the average battery round-trip efficiency for each BESS DUID in the last 7 days?":
        sql = f"""{with_ctes(UNIT_DISPATCH_CTE, UNIT_DIM_CTE)}
SELECT
    d.DUID,
    any(u.REGIONID) AS REGIONID,
    round(sum(greatest(d.TOTALCLEARED, 0) / 12.0), 2) AS discharge_mwh,
    round(sum(abs(least(d.TOTALCLEARED, 0)) / 12.0), 2) AS charge_mwh,
    round(
        sum(greatest(d.TOTALCLEARED, 0) / 12.0) /
        nullIf(sum(abs(least(d.TOTALCLEARED, 0)) / 12.0), 0),
        3
    ) AS round_trip_efficiency_proxy
FROM unit_dispatch d
INNER JOIN unit_dim u ON u.DUID = d.DUID
WHERE d.SETTLEMENTDATE >= now() - INTERVAL 7 DAY
  AND u.IS_STORAGE = 1
GROUP BY d.DUID
ORDER BY round_trip_efficiency_proxy DESC"""
        proxy_note = (
            "This uses cleared discharge divided by cleared charge as a market-dispatch proxy, "
            "not a metered electrochemical round-trip efficiency measurement. "
            + note
        )
        return QuerySpec(q, slug, "proxy", sql, proxy_note, "Battery round-trip efficiency proxy")

    if q == "Which batteries had the highest utilisation rate?":
        sql = f"""{with_ctes(UNIT_DISPATCH_CTE, UNIT_DIM_CTE)}
SELECT
    d.DUID,
    any(u.REGIONID) AS REGIONID,
    any(u.MAXCAPACITY_MW) AS max_capacity_mw,
    round(avg(abs(d.TOTALCLEARED)), 2) AS avg_abs_dispatch_mw,
    round(avg(abs(d.TOTALCLEARED)) / nullIf(any(u.MAXCAPACITY_MW), 0), 3) AS utilisation_rate
FROM unit_dispatch d
INNER JOIN unit_dim u ON u.DUID = d.DUID
WHERE u.IS_STORAGE = 1
GROUP BY d.DUID
ORDER BY utilisation_rate DESC
LIMIT 20"""
        return QuerySpec(q, slug, "proxy", sql, note, "Battery utilisation rates")

    if q == "What was the peak charge and discharge rate for each battery DUID yesterday?":
        sql = f"""{with_ctes(UNIT_DISPATCH_CTE, UNIT_DIM_CTE)}
SELECT
    d.DUID,
    any(u.REGIONID) AS REGIONID,
    round(max(greatest(d.TOTALCLEARED, 0)), 2) AS peak_discharge_mw,
    round(max(abs(least(d.TOTALCLEARED, 0))), 2) AS peak_charge_mw
FROM unit_dispatch d
INNER JOIN unit_dim u ON u.DUID = d.DUID
WHERE toDate(d.SETTLEMENTDATE) = yesterday()
  AND u.IS_STORAGE = 1
GROUP BY d.DUID
ORDER BY peak_discharge_mw DESC, peak_charge_mw DESC"""
        return QuerySpec(q, slug, "proxy", sql, note, "Peak battery charge and discharge rates")

    if q == "How wide was the NSW-VIC price spread each day over the last week?":
        sql = f"""{with_ctes(
            PRICE_DISPATCH_CTE,
            """
paired AS (
    SELECT
        toDate(SETTLEMENTDATE) AS trading_day,
        avgIf(RRP, REGIONID = 'NSW1') AS nsw_rrp,
        avgIf(RRP, REGIONID = 'VIC1') AS vic_rrp
    FROM price_dispatch
    WHERE SETTLEMENTDATE >= now() - INTERVAL 7 DAY
    GROUP BY trading_day
)
"""
        )}
SELECT
    trading_day,
    round(nsw_rrp - vic_rrp, 2) AS nsw_vic_spread_mwh
FROM paired
ORDER BY trading_day"""
        return QuerySpec(q, slug, "ready", sql, note, "NSW-VIC daily price spread")

    if q == "Which interconnectors had the highest average flow in the last 7 days?":
        sql = f"""{with_ctes(INTERCONNECTOR_CTE)}
SELECT
    INTERCONNECTORID,
    round(avg(abs(MWFLOW)), 2) AS avg_abs_flow_mw
FROM interconnector_dispatch
WHERE SETTLEMENTDATE >= now() - INTERVAL 7 DAY
GROUP BY INTERCONNECTORID
ORDER BY avg_abs_flow_mw DESC"""
        return QuerySpec(q, slug, "ready", sql, note, "Average interconnector flow")

    if q == "What was the average price spread across each interconnector pair?":
        sql = f"""{with_ctes(
            INTERCONNECTOR_CTE,
            PRICE_DISPATCH_CTE,
            """
prices AS (
    SELECT
        SETTLEMENTDATE,
        avgIf(RRP, REGIONID = 'NSW1') AS NSW1,
        avgIf(RRP, REGIONID = 'QLD1') AS QLD1,
        avgIf(RRP, REGIONID = 'VIC1') AS VIC1,
        avgIf(RRP, REGIONID = 'SA1') AS SA1,
        avgIf(RRP, REGIONID = 'TAS1') AS TAS1
    FROM price_dispatch
    GROUP BY SETTLEMENTDATE
)
"""
        )}
SELECT
    {INTERCONNECTOR_REGION_CASE} AS region_pair,
    round(avg(
        CASE
            WHEN region_pair = 'NSW1-QLD1' THEN abs(NSW1 - QLD1)
            WHEN region_pair = 'VIC1-NSW1' THEN abs(VIC1 - NSW1)
            WHEN region_pair = 'VIC1-SA1' THEN abs(VIC1 - SA1)
            WHEN region_pair = 'TAS1-VIC1' THEN abs(TAS1 - VIC1)
            ELSE NULL
        END
    ), 2) AS avg_price_spread_mwh
FROM interconnector_dispatch i
LEFT JOIN prices p ON p.SETTLEMENTDATE = i.SETTLEMENTDATE
GROUP BY region_pair
ORDER BY avg_price_spread_mwh DESC"""
        return QuerySpec(q, slug, "proxy", sql, note, "Average price spread by interconnector pair")

    if q == "When were interconnectors at their flow limit in the last 7 days?":
        sql = f"""{with_ctes(INTERCONNECTOR_CTE)}
SELECT
    SETTLEMENTDATE,
    INTERCONNECTORID,
    round(MWFLOW, 2) AS flow_mw,
    round(EXPORTLIMIT, 2) AS export_limit_mw,
    round(IMPORTLIMIT, 2) AS import_limit_mw
FROM interconnector_dispatch
WHERE SETTLEMENTDATE >= now() - INTERVAL 7 DAY
  AND (
      abs(MWFLOW - EXPORTLIMIT) <= 1
      OR abs(MWFLOW + IMPORTLIMIT) <= 1
      OR abs(abs(MWFLOW) - greatest(EXPORTLIMIT, IMPORTLIMIT)) <= 1
  )
ORDER BY SETTLEMENTDATE DESC, INTERCONNECTORID"""
        return QuerySpec(q, slug, "proxy", sql, note, "Interconnectors at or near flow limits")

    if q == "How correlated are prices between adjacent regions?":
        sql = f"""{with_ctes(
            PRICE_DISPATCH_CTE,
            """
paired AS (
    SELECT
        SETTLEMENTDATE,
        avgIf(RRP, REGIONID = 'NSW1') AS NSW1,
        avgIf(RRP, REGIONID = 'QLD1') AS QLD1,
        avgIf(RRP, REGIONID = 'VIC1') AS VIC1,
        avgIf(RRP, REGIONID = 'SA1') AS SA1,
        avgIf(RRP, REGIONID = 'TAS1') AS TAS1
    FROM price_dispatch
    GROUP BY SETTLEMENTDATE
)
"""
        )}
SELECT 'NSW1-QLD1' AS region_pair, corr(NSW1, QLD1) AS price_correlation FROM paired
UNION ALL
SELECT 'VIC1-NSW1' AS region_pair, corr(VIC1, NSW1) AS price_correlation FROM paired
UNION ALL
SELECT 'VIC1-SA1' AS region_pair, corr(VIC1, SA1) AS price_correlation FROM paired
UNION ALL
SELECT 'TAS1-VIC1' AS region_pair, corr(TAS1, VIC1) AS price_correlation FROM paired
ORDER BY price_correlation DESC"""
        return QuerySpec(q, slug, "ready", sql, note, "Price correlation across adjacent regions")

    if q == "What was the total FCAS cost by region and market over the last 7 days?":
        sql = f"""{with_ctes(REGION_DISPATCH_CTE, PRICE_DISPATCH_CTE)}
SELECT
    d.REGIONID,
    market,
    round(sum(enabled_mw / 12.0 * price_mwh), 2) AS fcas_cost_proxy
FROM (
    SELECT SETTLEMENTDATE, REGIONID, 'RAISE6SEC' AS market, RAISE6SECDISPATCH AS enabled_mw FROM region_dispatch
    UNION ALL SELECT SETTLEMENTDATE, REGIONID, 'RAISE60SEC', RAISE60SECDISPATCH FROM region_dispatch
    UNION ALL SELECT SETTLEMENTDATE, REGIONID, 'RAISE5MIN', RAISE5MINDISPATCH FROM region_dispatch
    UNION ALL SELECT SETTLEMENTDATE, REGIONID, 'RAISEREG', RAISEREGLOCALDISPATCH FROM region_dispatch
    UNION ALL SELECT SETTLEMENTDATE, REGIONID, 'LOWER6SEC', LOWER6SECDISPATCH FROM region_dispatch
    UNION ALL SELECT SETTLEMENTDATE, REGIONID, 'LOWER60SEC', LOWER60SECDISPATCH FROM region_dispatch
    UNION ALL SELECT SETTLEMENTDATE, REGIONID, 'LOWER5MIN', LOWER5MINDISPATCH FROM region_dispatch
    UNION ALL SELECT SETTLEMENTDATE, REGIONID, 'LOWERREG', LOWERREGLOCALDISPATCH FROM region_dispatch
) d
LEFT JOIN (
    SELECT SETTLEMENTDATE, REGIONID, 'RAISE6SEC' AS market, RAISE6SECRRP AS price_mwh FROM price_dispatch
    UNION ALL SELECT SETTLEMENTDATE, REGIONID, 'RAISE60SEC', RAISE60SECRRP FROM price_dispatch
    UNION ALL SELECT SETTLEMENTDATE, REGIONID, 'RAISE5MIN', RAISE5MINRRP FROM price_dispatch
    UNION ALL SELECT SETTLEMENTDATE, REGIONID, 'RAISEREG', RAISEREGRRP FROM price_dispatch
    UNION ALL SELECT SETTLEMENTDATE, REGIONID, 'LOWER6SEC', LOWER6SECRRP FROM price_dispatch
    UNION ALL SELECT SETTLEMENTDATE, REGIONID, 'LOWER60SEC', LOWER60SECRRP FROM price_dispatch
    UNION ALL SELECT SETTLEMENTDATE, REGIONID, 'LOWER5MIN', LOWER5MINRRP FROM price_dispatch
    UNION ALL SELECT SETTLEMENTDATE, REGIONID, 'LOWERREG', LOWERREGRRP FROM price_dispatch
) p ON p.SETTLEMENTDATE = d.SETTLEMENTDATE AND p.REGIONID = d.REGIONID AND p.market = d.market
WHERE d.SETTLEMENTDATE >= now() - INTERVAL 7 DAY
GROUP BY d.REGIONID, market
ORDER BY d.REGIONID, fcas_cost_proxy DESC"""
        proxy_note = (
            "This estimates FCAS cost as enabled MW times FCAS price for each interval. "
            "It is a market-operations proxy, not a settlement-grade FCAS cost calculation. "
            + note
        )
        return QuerySpec(q, slug, "proxy", sql, proxy_note, "FCAS cost by region and market")

    if q == "Which FCAS market had the highest average price this week?":
        sql = f"""{with_ctes(PRICE_DISPATCH_CTE)}
SELECT market, round(avg(price_mwh), 2) AS avg_price_mwh
FROM (
    SELECT SETTLEMENTDATE, 'RAISE6SEC' AS market, RAISE6SECRRP AS price_mwh FROM price_dispatch
    UNION ALL SELECT SETTLEMENTDATE, 'RAISE60SEC', RAISE60SECRRP FROM price_dispatch
    UNION ALL SELECT SETTLEMENTDATE, 'RAISE5MIN', RAISE5MINRRP FROM price_dispatch
    UNION ALL SELECT SETTLEMENTDATE, 'RAISEREG', RAISEREGRRP FROM price_dispatch
    UNION ALL SELECT SETTLEMENTDATE, 'LOWER6SEC', LOWER6SECRRP FROM price_dispatch
    UNION ALL SELECT SETTLEMENTDATE, 'LOWER60SEC', LOWER60SECRRP FROM price_dispatch
    UNION ALL SELECT SETTLEMENTDATE, 'LOWER5MIN', LOWER5MINRRP FROM price_dispatch
    UNION ALL SELECT SETTLEMENTDATE, 'LOWERREG', LOWERREGRRP FROM price_dispatch
) f
WHERE SETTLEMENTDATE >= date_trunc('week', now())
GROUP BY market
ORDER BY avg_price_mwh DESC"""
        return QuerySpec(q, slug, "ready", sql, note, "Average FCAS price by market")

    if q == "How much FCAS enablement did each region provide in the last 7 days?":
        sql = f"""{with_ctes(REGION_DISPATCH_CTE)}
SELECT
    REGIONID,
    market,
    round(sum(enabled_mw / 12.0), 2) AS enabled_mwh_proxy
FROM (
    SELECT SETTLEMENTDATE, REGIONID, 'RAISE' AS market, RAISE6SECDISPATCH + RAISE60SECDISPATCH + RAISE5MINDISPATCH + RAISEREGLOCALDISPATCH AS enabled_mw FROM region_dispatch
    UNION ALL
    SELECT SETTLEMENTDATE, REGIONID, 'LOWER' AS market, LOWER6SECDISPATCH + LOWER60SECDISPATCH + LOWER5MINDISPATCH + LOWERREGLOCALDISPATCH AS enabled_mw FROM region_dispatch
) f
WHERE SETTLEMENTDATE >= now() - INTERVAL 7 DAY
GROUP BY REGIONID, market
ORDER BY REGIONID, market"""
        return QuerySpec(q, slug, "proxy", sql, note, "FCAS enablement by region")

    if q == "Which DUIDs provided the most FCAS across all markets in the last day?":
        sql = f"""{with_ctes(UNIT_DISPATCH_CTE, UNIT_DIM_CTE)}
SELECT
    d.DUID,
    any(u.REGIONID) AS REGIONID,
    any(u.FUEL_TYPE) AS FUEL_TYPE,
    round(sum((FCAS_RAISE_TOTAL + FCAS_LOWER_TOTAL) / 12.0), 2) AS fcas_enabled_mwh_proxy
FROM unit_dispatch d
LEFT JOIN unit_dim u ON u.DUID = d.DUID
WHERE d.SETTLEMENTDATE >= now() - INTERVAL 1 DAY
GROUP BY d.DUID
ORDER BY fcas_enabled_mwh_proxy DESC
LIMIT 20"""
        return QuerySpec(q, slug, "proxy", sql, note, "Top FCAS-providing DUIDs")

    if q == "What is the ratio of FCAS price to energy price by region over time?":
        sql = f"""{with_ctes(PRICE_DISPATCH_CTE)}
SELECT
    toDate(SETTLEMENTDATE) AS trading_day,
    REGIONID,
    round(avg((RAISE6SECRRP + RAISE60SECRRP + RAISE5MINRRP + RAISEREGRRP + LOWER6SECRRP + LOWER60SECRRP + LOWER5MINRRP + LOWERREGRRP) / 8.0), 2) AS avg_fcas_price,
    round(avg(RRP), 2) AS avg_energy_price,
    round(avg((RAISE6SECRRP + RAISE60SECRRP + RAISE5MINRRP + RAISEREGRRP + LOWER6SECRRP + LOWER60SECRRP + LOWER5MINRRP + LOWERREGRRP) / 8.0) / nullIf(avg(RRP), 0), 3) AS fcas_to_energy_ratio
FROM price_dispatch
GROUP BY trading_day, REGIONID
ORDER BY trading_day, REGIONID"""
        return QuerySpec(q, slug, "proxy", sql, note, "FCAS to energy price ratio")

    if q == "Which participants own the most registered generation capacity?":
        sql = f"""{with_ctes(UNIT_DIM_CTE)}
SELECT
    coalesce(EFFECTIVE_PARTICIPANTID, PARTICIPANTID, 'UNKNOWN') AS participant_id,
    round(sum(REGISTEREDCAPACITY_MW), 2) AS registered_capacity_mw
FROM unit_dim
WHERE coalesce(DISPATCHTYPE, '') = 'GENERATOR'
GROUP BY participant_id
ORDER BY registered_capacity_mw DESC
LIMIT 25"""
        return QuerySpec(q, slug, "ready", sql, note, "Participants with the most registered capacity")

    if q == "How many DUIDs does each participant have and in which regions?":
        sql = f"""{with_ctes(UNIT_DIM_CTE)}
SELECT
    coalesce(EFFECTIVE_PARTICIPANTID, PARTICIPANTID, 'UNKNOWN') AS participant_id,
    REGIONID,
    countDistinct(DUID) AS duid_count
FROM unit_dim
GROUP BY participant_id, REGIONID
ORDER BY participant_id, REGIONID"""
        return QuerySpec(q, slug, "ready", sql, note, "DUID counts by participant and region")

    if q == "What is the total registered capacity by fuel type in each region?":
        sql = f"""{with_ctes(UNIT_DIM_CTE)}
SELECT
    REGIONID,
    FUEL_TYPE,
    round(sum(REGISTEREDCAPACITY_MW), 2) AS registered_capacity_mw
FROM unit_dim
WHERE coalesce(DISPATCHTYPE, '') = 'GENERATOR'
GROUP BY REGIONID, FUEL_TYPE
ORDER BY REGIONID, registered_capacity_mw DESC"""
        return QuerySpec(q, slug, "ready", sql, note, "Registered capacity by fuel and region")

    if q == "Which stations have the largest installed capacity?":
        sql = f"""{with_ctes(UNIT_DIM_CTE)}
SELECT
    STATIONID,
    any(STATIONNAME) AS station_name,
    any(REGIONID) AS REGIONID,
    round(sum(REGISTEREDCAPACITY_MW), 2) AS installed_capacity_mw
FROM unit_dim
WHERE coalesce(DISPATCHTYPE, '') = 'GENERATOR'
GROUP BY STATIONID
ORDER BY installed_capacity_mw DESC
LIMIT 25"""
        return QuerySpec(q, slug, "ready", sql, note, "Largest stations by installed capacity")

    if q == "How many batteries are registered in each region and what is their total capacity?":
        sql = f"""{with_ctes(UNIT_DIM_CTE)}
SELECT
    REGIONID,
    countDistinct(DUID) AS battery_duid_count,
    round(sum(REGISTEREDCAPACITY_MW), 2) AS battery_capacity_mw,
    round(sum(MAXSTORAGECAPACITY_MWH), 2) AS battery_storage_mwh
FROM unit_dim
WHERE IS_STORAGE = 1
GROUP BY REGIONID
ORDER BY battery_capacity_mw DESC"""
        return QuerySpec(q, slug, "ready", sql, note, "Registered batteries by region")

    if q == "What was the estimated gross merchant revenue for the top 20 generators this week?":
        sql = f"""{with_ctes(ACTUAL_GEN_CTE, PRICE_DISPATCH_CTE, UNIT_DIM_CTE)}
SELECT
    g.DUID,
    any(u.REGIONID) AS REGIONID,
    any(u.FUEL_TYPE) AS FUEL_TYPE,
    round(sum(g.MWH_READING * p.RRP), 2) AS gross_revenue_proxy
FROM actual_gen g
INNER JOIN unit_dim u ON u.DUID = g.DUID
LEFT JOIN price_dispatch p ON p.SETTLEMENTDATE = g.INTERVAL_DATETIME AND p.REGIONID = u.REGIONID
WHERE g.INTERVAL_DATETIME >= date_trunc('week', now())
  AND coalesce(u.DISPATCHTYPE, '') = 'GENERATOR'
GROUP BY g.DUID
ORDER BY gross_revenue_proxy DESC
LIMIT 20"""
        proxy_note = (
            "Gross merchant revenue is estimated as interval energy multiplied by regional spot price. "
            "This excludes contracts, settlement adjustments, losses, FCAS revenue, and bidding effects. "
            + note
        )
        return QuerySpec(q, slug, "proxy", sql, proxy_note, "Estimated gross merchant revenue")

    if q == "Which generators earned the most from high-price intervals?":
        sql = f"""{with_ctes(ACTUAL_GEN_CTE, PRICE_DISPATCH_CTE, UNIT_DIM_CTE)}
SELECT
    g.DUID,
    any(u.REGIONID) AS REGIONID,
    any(u.FUEL_TYPE) AS FUEL_TYPE,
    round(sum(g.MWH_READING * p.RRP), 2) AS revenue_in_high_price_intervals
FROM actual_gen g
INNER JOIN unit_dim u ON u.DUID = g.DUID
LEFT JOIN price_dispatch p ON p.SETTLEMENTDATE = g.INTERVAL_DATETIME AND p.REGIONID = u.REGIONID
WHERE p.RRP >= 300
  AND coalesce(u.DISPATCHTYPE, '') = 'GENERATOR'
GROUP BY g.DUID
ORDER BY revenue_in_high_price_intervals DESC
LIMIT 20"""
        return QuerySpec(q, slug, "proxy", sql, note, "Revenue from high-price intervals")

    if q == "What was the approximate revenue per MWh for each fuel type?":
        sql = f"""{with_ctes(ACTUAL_GEN_CTE, PRICE_DISPATCH_CTE, UNIT_DIM_CTE)}
SELECT
    any(u.FUEL_TYPE) AS FUEL_TYPE,
    round(sum(g.MWH_READING * p.RRP), 2) AS revenue_proxy,
    round(sum(g.MWH_READING), 2) AS energy_mwh,
    round(sum(g.MWH_READING * p.RRP) / nullIf(sum(g.MWH_READING), 0), 2) AS revenue_per_mwh_proxy
FROM actual_gen g
INNER JOIN unit_dim u ON u.DUID = g.DUID
LEFT JOIN price_dispatch p ON p.SETTLEMENTDATE = g.INTERVAL_DATETIME AND p.REGIONID = u.REGIONID
WHERE coalesce(u.DISPATCHTYPE, '') = 'GENERATOR'
GROUP BY u.FUEL_TYPE
ORDER BY revenue_per_mwh_proxy DESC"""
        return QuerySpec(q, slug, "proxy", sql, note, "Approximate revenue per MWh by fuel")

    if q == "Which batteries had the highest estimated arbitrage revenue this week?":
        sql = f"""{with_ctes(UNIT_DISPATCH_CTE, PRICE_DISPATCH_CTE, UNIT_DIM_CTE)}
SELECT
    d.DUID,
    any(u.REGIONID) AS REGIONID,
    round(sum(
        CASE
            WHEN d.TOTALCLEARED >= 0 THEN (d.TOTALCLEARED / 12.0) * p.RRP
            ELSE -(abs(d.TOTALCLEARED) / 12.0) * p.RRP
        END
    ), 2) AS arbitrage_revenue_proxy
FROM unit_dispatch d
INNER JOIN unit_dim u ON u.DUID = d.DUID
LEFT JOIN price_dispatch p ON p.SETTLEMENTDATE = d.SETTLEMENTDATE AND p.REGIONID = u.REGIONID
WHERE d.SETTLEMENTDATE >= date_trunc('week', now())
  AND u.IS_STORAGE = 1
GROUP BY d.DUID
ORDER BY arbitrage_revenue_proxy DESC
LIMIT 20"""
        return QuerySpec(q, slug, "proxy", sql, note, "Estimated battery arbitrage revenue")

    if q == "How does generator revenue correlate with capacity factor?":
        sql = f"""{with_ctes(
            ACTUAL_GEN_CTE,
            PRICE_DISPATCH_CTE,
            UNIT_DIM_CTE,
            """
generator_metrics AS (
    SELECT
        g.DUID AS DUID,
        any(u.FUEL_TYPE) AS FUEL_TYPE,
        any(u.REGISTEREDCAPACITY_MW) AS registered_capacity_mw,
        sum(g.MWH_READING) AS energy_mwh,
        sum(g.MWH_READING * p.RRP) AS revenue_proxy,
        dateDiff('hour', min(g.INTERVAL_DATETIME), max(g.INTERVAL_DATETIME)) + 1 AS span_hours
    FROM actual_gen g
    INNER JOIN unit_dim u ON u.DUID = g.DUID
    LEFT JOIN price_dispatch p ON p.SETTLEMENTDATE = g.INTERVAL_DATETIME AND p.REGIONID = u.REGIONID
    WHERE coalesce(u.DISPATCHTYPE, '') = 'GENERATOR'
    GROUP BY g.DUID
)
"""
        )}
SELECT
    DUID,
    FUEL_TYPE,
    round(revenue_proxy, 2) AS revenue_proxy,
    round(energy_mwh / nullIf(registered_capacity_mw * span_hours, 0), 4) AS capacity_factor
FROM generator_metrics
ORDER BY revenue_proxy DESC
LIMIT 100"""
        return QuerySpec(q, slug, "proxy", sql, note, "Revenue versus capacity factor")

    if q == "What were the top 10 negative-price DUIDs (generators still running during negative prices)?":
        sql = f"""{with_ctes(ACTUAL_GEN_CTE, PRICE_DISPATCH_CTE, UNIT_DIM_CTE)}
SELECT
    g.DUID,
    any(u.REGIONID) AS REGIONID,
    any(u.FUEL_TYPE) AS FUEL_TYPE,
    count() AS negative_price_intervals_running,
    round(sum(g.MWH_READING), 2) AS energy_mwh_at_negative_prices
FROM actual_gen g
INNER JOIN unit_dim u ON u.DUID = g.DUID
LEFT JOIN price_dispatch p ON p.SETTLEMENTDATE = g.INTERVAL_DATETIME AND p.REGIONID = u.REGIONID
WHERE p.RRP < 0
  AND g.MWH_READING > 0
  AND coalesce(u.DISPATCHTYPE, '') = 'GENERATOR'
GROUP BY g.DUID
ORDER BY negative_price_intervals_running DESC, energy_mwh_at_negative_prices DESC
LIMIT 10"""
        return QuerySpec(q, slug, "ready", sql, note, "Top negative-price running generators")

    if q == "How many dispatch intervals showed price separation between regions in the last 7 days?":
        sql = f"""{with_ctes(
            PRICE_DISPATCH_CTE,
            """
interval_spreads AS (
    SELECT
        SETTLEMENTDATE,
        max(RRP) - min(RRP) AS price_spread
    FROM price_dispatch
    WHERE SETTLEMENTDATE >= now() - INTERVAL 7 DAY
    GROUP BY SETTLEMENTDATE
)
"""
        )}
SELECT countIf(price_spread > 1) AS separated_intervals
FROM interval_spreads"""
        return QuerySpec(q, slug, "ready", sql, note, "Intervals with regional price separation")

    if q == "Which constraints were most frequently binding in the last 30 days?":
        sql = f"""{with_ctes(CONSTRAINT_CTE)}
SELECT
    CONSTRAINTID,
    countIf(abs(MARGINALVALUE) > 0.01) AS binding_intervals,
    round(avgIf(MARGINALVALUE, abs(MARGINALVALUE) > 0.01), 2) AS avg_binding_shadow_price
FROM constraint_dispatch
WHERE SETTLEMENTDATE >= now() - INTERVAL 30 DAY
GROUP BY CONSTRAINTID
ORDER BY binding_intervals DESC
LIMIT 25"""
        return QuerySpec(q, slug, "ready", sql, note, "Most frequently binding constraints")

    if q == "What was the average constraint shadow price by constraint type?":
        sql = f"""{with_ctes(CONSTRAINT_CTE)}
SELECT
    splitByChar('_', ifNull(CONSTRAINTID, 'UNKNOWN'))[1] AS constraint_type_proxy,
    round(avg(MARGINALVALUE), 2) AS avg_shadow_price
FROM constraint_dispatch
GROUP BY constraint_type_proxy
ORDER BY avg_shadow_price DESC"""
        proxy_note = (
            "Constraint type is proxied from the leading token in CONSTRAINTID, because a clean semantic "
            "constraint-type dimension is not available yet. "
            + note
        )
        return QuerySpec(q, slug, "proxy", sql, proxy_note, "Constraint shadow price by type")

    if q == "How did the dispatch engine's solution quality change over the last 30 days?":
        sql = f"""{with_ctes(REGION_DISPATCH_CTE)}
SELECT
    toDate(SETTLEMENTDATE) AS trading_day,
    round(avg(abs(RRP)), 2) AS avg_abs_price,
    round(avg(abs(TOTALDEMAND - DEMANDFORECAST)), 2) AS avg_demand_error_mw,
    round(avg(abs(EXCESSGENERATION)), 2) AS avg_excess_generation_mw
FROM region_dispatch
WHERE SETTLEMENTDATE >= now() - INTERVAL 30 DAY
GROUP BY trading_day
ORDER BY trading_day"""
        proxy_note = (
            "This uses observable dispatch outcomes as a proxy for solution quality because a dedicated "
            "solver-quality metric is not exposed in the semantic layer. "
            + note
        )
        return QuerySpec(q, slug, "proxy", sql, proxy_note, "Dispatch engine solution-quality proxy")

    if q == "What is the current installed solar capacity vs 12 months ago by region?":
        sql = f"""{with_ctes(UNIT_DIM_CTE)}
SELECT
    REGIONID,
    round(sumIf(REGISTEREDCAPACITY_MW, lower(FUEL_TYPE) = 'solar'), 2) AS current_solar_capacity_mw
FROM unit_dim
GROUP BY REGIONID
ORDER BY current_solar_capacity_mw DESC"""
        proxy_note = (
            "The semantic layer currently exposes current unit state cleanly; the 12-month comparison is omitted "
            "until a historical registration snapshot mart is built. "
            + note
        )
        return QuerySpec(q, slug, "limited", sql, proxy_note, "Current installed solar capacity by region")

    if q == "How has the wind generation share changed quarter-over-quarter?":
        sql = f"""{with_ctes(
            ACTUAL_GEN_CTE,
            UNIT_DIM_CTE,
            """
quarterly AS (
    SELECT
        toStartOfQuarter(g.INTERVAL_DATETIME) AS quarter_start,
        sumIf(g.MWH_READING, lower(u.FUEL_TYPE) = 'wind') AS wind_mwh,
        sum(g.MWH_READING) AS total_mwh
    FROM actual_gen g
    INNER JOIN unit_dim u ON u.DUID = g.DUID
    GROUP BY quarter_start
)
"""
        )}
SELECT
    quarter_start,
    round(wind_mwh, 2) AS wind_mwh,
    round(total_mwh, 2) AS total_mwh,
    round(wind_mwh / nullIf(total_mwh, 0), 4) AS wind_share
FROM quarterly
ORDER BY quarter_start"""
        return QuerySpec(q, slug, "limited", sql, note, "Quarter-over-quarter wind share")

    if q == "What is the monthly trend in battery registrations and capacity?":
        sql = """
WITH battery_events AS (
    SELECT
        toStartOfMonth(EFFECTIVEDATE) AS month_start,
        DUID,
        avg(REGISTEREDCAPACITY) AS registered_capacity_mw
    FROM semantic.participant_registration_dudetail
    WHERE MAXSTORAGECAPACITY > 0 OR DISPATCHTYPE = 'BIDIRECTIONAL'
    GROUP BY month_start, DUID
)
SELECT
    month_start,
    countDistinct(DUID) AS battery_registrations,
    round(sum(registered_capacity_mw), 2) AS registered_capacity_mw
FROM battery_events
GROUP BY month_start
ORDER BY month_start"""
        return QuerySpec(q, slug, "ready", sql, note, "Monthly battery registration trend")

    if q == "How has the demand profile shape changed year-over-year?":
        sql = f"""{with_ctes(REGION_DISPATCH_CTE)}
SELECT
    toYear(SETTLEMENTDATE) AS year,
    toHour(SETTLEMENTDATE) AS hour_of_day,
    REGIONID,
    round(avg(TOTALDEMAND), 2) AS avg_demand_mw
FROM region_dispatch
GROUP BY year, hour_of_day, REGIONID
ORDER BY year, hour_of_day, REGIONID"""
        return QuerySpec(q, slug, "limited", sql, note, "Year-over-year demand shape")

    if q == "What is the trend in average daily price by region over the last 12 months?":
        sql = f"""{with_ctes(PRICE_DISPATCH_CTE)}
SELECT
    toDate(SETTLEMENTDATE) AS trading_day,
    REGIONID,
    round(avg(RRP), 2) AS avg_daily_price
FROM price_dispatch
WHERE SETTLEMENTDATE >= now() - INTERVAL 12 MONTH
GROUP BY trading_day, REGIONID
ORDER BY trading_day, REGIONID"""
        return QuerySpec(q, slug, "limited", sql, note, "Average daily price trend")

    if q == "How does actual generation compare to PASA forecast by fuel type?":
        sql = diagnostic_sql(
            "PASA-to-actual comparison needs a reconciled fuel-typed PASA fact mart and clean alignment logic between PASA availability and interval generation."
        )
        return QuerySpec(q, slug, "blocked", sql, external_dataset_note("The current semantic layer does not yet expose a clean PASA-versus-actual comparison surface."), "Blocked")

    if q == "What was the demand forecast error distribution for each region?":
        sql = f"""{with_ctes(REGION_DISPATCH_CTE)}
SELECT
    REGIONID,
    round(TOTALDEMAND - DEMANDFORECAST, 2) AS forecast_error_mw
FROM region_dispatch
ORDER BY REGIONID, SETTLEMENTDATE"""
        return QuerySpec(q, slug, "ready", sql, note, "Demand forecast error distribution")

    if q == "How accurate was the 5-minute pre-dispatch price forecast?":
        sql = f"""{with_ctes(
            P5_REGION_CTE,
            PRICE_DISPATCH_CTE,
            """
latest_forecast AS (
    SELECT
        INTERVAL_DATETIME,
        REGIONID,
        argMax(RRP, RUN_DATETIME) AS forecast_rrp
    FROM p5_region
    WHERE RUN_DATETIME <= INTERVAL_DATETIME
    GROUP BY INTERVAL_DATETIME, REGIONID
)
"""
        )}
SELECT
    l.INTERVAL_DATETIME,
    l.REGIONID,
    round(l.forecast_rrp, 2) AS forecast_rrp,
    round(p.RRP, 2) AS actual_rrp,
    round(l.forecast_rrp - p.RRP, 2) AS error_rrp
FROM latest_forecast l
LEFT JOIN price_dispatch p ON p.SETTLEMENTDATE = l.INTERVAL_DATETIME AND p.REGIONID = l.REGIONID
ORDER BY l.INTERVAL_DATETIME, l.REGIONID"""
        return QuerySpec(q, slug, "proxy", sql, note, "5-minute pre-dispatch price forecast accuracy")

    if q == "What was the 30-minute pre-dispatch forecast vs actual for each region?":
        sql = f"""{with_ctes(PREDISPATCH_PRICE_CTE, PRICE_DISPATCH_CTE)}
SELECT
    p.DATETIME,
    p.REGIONID,
    round(p.RRP, 2) AS predispatch_rrp,
    round(a.RRP, 2) AS actual_rrp,
    round(p.RRP - a.RRP, 2) AS error_rrp
FROM predispatch_price p
LEFT JOIN price_dispatch a ON a.SETTLEMENTDATE = p.DATETIME AND a.REGIONID = p.REGIONID
ORDER BY p.DATETIME, p.REGIONID"""
        return QuerySpec(q, slug, "proxy", sql, note, "30-minute predispatch forecast versus actual")

    if q == "How far ahead does the pre-dispatch forecast diverge significantly from actual?":
        sql = diagnostic_sql(
            "This needs a forecast-horizon mart that keeps multiple predispatch runs for the same target interval."
        )
        return QuerySpec(q, slug, "blocked", sql, external_dataset_note("The current semantic views collapse most forecast horizons, so ahead-time divergence is not recoverable cleanly yet."), "Blocked")

    if q == "What were the top 10 price events (highest 5-minute prices) in the last 30 days?":
        sql = f"""{with_ctes(PRICE_DISPATCH_CTE)}
SELECT
    SETTLEMENTDATE,
    REGIONID,
    round(RRP, 2) AS spot_price_mwh
FROM price_dispatch
WHERE SETTLEMENTDATE >= now() - INTERVAL 30 DAY
ORDER BY RRP DESC
LIMIT 10"""
        return QuerySpec(q, slug, "ready", sql, note, "Top price events")

    if q == "What was the generation mix during each major price event?":
        sql = f"""{with_ctes(
            PRICE_DISPATCH_CTE,
            ACTUAL_GEN_CTE,
            UNIT_DIM_CTE,
            """
top_events AS (
    SELECT SETTLEMENTDATE, REGIONID, RRP
    FROM price_dispatch
    ORDER BY RRP DESC
    LIMIT 10
)
"""
        )}
SELECT
    e.SETTLEMENTDATE AS event_time,
    e.REGIONID,
    u.FUEL_TYPE,
    round(sum(g.MWH_READING), 2) AS generation_mwh
FROM top_events e
LEFT JOIN actual_gen g ON g.INTERVAL_DATETIME = e.SETTLEMENTDATE
LEFT JOIN unit_dim u ON u.DUID = g.DUID AND u.REGIONID = e.REGIONID
GROUP BY event_time, e.REGIONID, u.FUEL_TYPE
ORDER BY event_time, generation_mwh DESC"""
        return QuerySpec(q, slug, "proxy", sql, note, "Generation mix during price events")

    if q == "Which generators responded to price spikes and how quickly?":
        sql = diagnostic_sql(
            "This needs event-window response logic with pre- and post-spike ramp comparison."
        )
        return QuerySpec(q, slug, "blocked", sql, external_dataset_note("Response speed to price spikes is not yet implemented in the report harness."), "Blocked")

    if q == "Was there a correlation between demand forecast errors and price spikes?":
        sql = f"""{with_ctes(
            REGION_DISPATCH_CTE,
            PRICE_DISPATCH_CTE,
            """
joined AS (
    SELECT
        d.SETTLEMENTDATE,
        d.REGIONID,
        abs(d.TOTALDEMAND - d.DEMANDFORECAST) AS abs_forecast_error_mw,
        p.RRP AS price_rrp
    FROM region_dispatch d
    LEFT JOIN price_dispatch p ON p.SETTLEMENTDATE = d.SETTLEMENTDATE AND p.REGIONID = d.REGIONID
)
"""
        )}
SELECT
    REGIONID,
    corr(abs_forecast_error_mw, price_rrp) AS error_price_correlation
FROM joined
GROUP BY REGIONID
ORDER BY error_price_correlation DESC"""
        return QuerySpec(q, slug, "ready", sql, note, "Demand forecast error versus price correlation")

    if q == "Which region had the most frequent price cap events?":
        sql = f"""{with_ctes(PRICE_DISPATCH_CTE)}
SELECT
    REGIONID,
    countIf(RRP >= 15000) AS price_cap_events
FROM price_dispatch
GROUP BY REGIONID
ORDER BY price_cap_events DESC"""
        return QuerySpec(q, slug, "ready", sql, note, "Price cap events by region")

    if q == "How did generators change their bids day-over-day?":
        sql = f"""{with_ctes(BID_DAY_CTE)}
SELECT
    SETTLEMENTDATE,
    DUID,
    avg(PRICEBAND1 + PRICEBAND2 + PRICEBAND3 + PRICEBAND4 + PRICEBAND5 + PRICEBAND6 + PRICEBAND7 + PRICEBAND8 + PRICEBAND9 + PRICEBAND10) / 10.0 AS avg_bid_band_price
FROM bid_day
WHERE BIDTYPE = 'ENERGY'
GROUP BY SETTLEMENTDATE, DUID
ORDER BY SETTLEMENTDATE, DUID"""
        return QuerySpec(q, slug, "proxy", sql, note, "Day-over-day generator bid changes")

    if q == "What is the typical bid structure for each fuel type?":
        sql = f"""{with_ctes(BID_DAY_CTE, UNIT_DIM_CTE)}
SELECT
    u.FUEL_TYPE,
    round(avg(PRICEBAND1), 2) AS avg_priceband1,
    round(avg(PRICEBAND5), 2) AS avg_priceband5,
    round(avg(PRICEBAND10), 2) AS avg_priceband10
FROM bid_day b
LEFT JOIN unit_dim u ON u.DUID = b.DUID
WHERE b.BIDTYPE = 'ENERGY'
GROUP BY u.FUEL_TYPE
ORDER BY u.FUEL_TYPE"""
        return QuerySpec(q, slug, "proxy", sql, note, "Typical bid structure by fuel type")

    if q == "How do battery bid prices compare to gas peaker bid prices?":
        sql = f"""{with_ctes(BID_DAY_CTE, UNIT_DIM_CTE)}
SELECT
    CASE
        WHEN u.IS_STORAGE = 1 THEN 'Battery'
        WHEN lower(u.FUEL_TYPE) = 'natural gas (pipeline)' THEN 'Gas peaker'
        ELSE 'Other'
    END AS technology,
    round(avg(PRICEBAND1), 2) AS avg_priceband1,
    round(avg(PRICEBAND5), 2) AS avg_priceband5,
    round(avg(PRICEBAND10), 2) AS avg_priceband10
FROM bid_day b
LEFT JOIN unit_dim u ON u.DUID = b.DUID
WHERE b.BIDTYPE = 'ENERGY'
  AND (u.IS_STORAGE = 1 OR lower(u.FUEL_TYPE) = 'natural gas (pipeline)')
GROUP BY technology
ORDER BY technology"""
        return QuerySpec(q, slug, "proxy", sql, note, "Battery versus gas bid prices")

    if q == "Which generators rebid most frequently and why (volume changes)?":
        sql = f"""{with_ctes(BID_DAY_CTE)}
SELECT
    DUID,
    count() AS rebid_count,
    anyLast(REBIDEXPLANATION) AS latest_rebid_explanation
FROM bid_day
WHERE BIDTYPE = 'ENERGY'
  AND REBIDEXPLANATION IS NOT NULL
  AND REBIDEXPLANATION != ''
GROUP BY DUID
ORDER BY rebid_count DESC
LIMIT 25"""
        return QuerySpec(q, slug, "proxy", sql, note, "Most frequent rebidders")

    if q == "What was the average bid-weighted price by fuel type?":
        sql = f"""{with_ctes(BID_DAY_CTE, BID_PER_CTE, UNIT_DIM_CTE)}
SELECT
    u.FUEL_TYPE,
    round(avg(
        (b.PRICEBAND1 * p.BANDAVAIL1 + b.PRICEBAND2 * p.BANDAVAIL2 + b.PRICEBAND3 * p.BANDAVAIL3 +
         b.PRICEBAND4 * p.BANDAVAIL4 + b.PRICEBAND5 * p.BANDAVAIL5 + b.PRICEBAND6 * p.BANDAVAIL6 +
         b.PRICEBAND7 * p.BANDAVAIL7 + b.PRICEBAND8 * p.BANDAVAIL8 + b.PRICEBAND9 * p.BANDAVAIL9 +
         b.PRICEBAND10 * p.BANDAVAIL10) /
        nullIf(
            p.BANDAVAIL1 + p.BANDAVAIL2 + p.BANDAVAIL3 + p.BANDAVAIL4 + p.BANDAVAIL5 +
            p.BANDAVAIL6 + p.BANDAVAIL7 + p.BANDAVAIL8 + p.BANDAVAIL9 + p.BANDAVAIL10, 0
        )
    ), 2) AS avg_bid_weighted_price
FROM bid_day b
INNER JOIN bid_period p ON p.DUID = b.DUID AND p.SETTLEMENTDATE = b.SETTLEMENTDATE AND p.BIDTYPE = b.BIDTYPE
LEFT JOIN unit_dim u ON u.DUID = b.DUID
WHERE b.BIDTYPE = 'ENERGY'
GROUP BY u.FUEL_TYPE
ORDER BY avg_bid_weighted_price DESC"""
        return QuerySpec(q, slug, "proxy", sql, note, "Average bid-weighted price by fuel type")

    if q == "What is the approximate carbon intensity of generation by region over the last 7 days?":
        sql = f"""{with_ctes(ACTUAL_GEN_CTE, UNIT_DIM_CTE)}
SELECT
    u.REGIONID,
    round(sum(g.MWH_READING * coalesce(u.CO2E_EMISSIONS_FACTOR, 0)), 2) AS emissions_tco2e_proxy,
    round(sum(g.MWH_READING), 2) AS generation_mwh,
    round(sum(g.MWH_READING * coalesce(u.CO2E_EMISSIONS_FACTOR, 0)) / nullIf(sum(g.MWH_READING), 0), 3) AS tco2e_per_mwh
FROM actual_gen g
INNER JOIN unit_dim u ON u.DUID = g.DUID
WHERE g.INTERVAL_DATETIME >= now() - INTERVAL 7 DAY
GROUP BY u.REGIONID
ORDER BY tco2e_per_mwh ASC"""
        return QuerySpec(q, slug, "proxy", sql, note, "Approximate carbon intensity by region")

    if q == "Which region has the lowest emissions intensity?":
        sql = f"""{with_ctes(ACTUAL_GEN_CTE, UNIT_DIM_CTE)}
SELECT
    u.REGIONID,
    round(sum(g.MWH_READING * coalesce(u.CO2E_EMISSIONS_FACTOR, 0)) / nullIf(sum(g.MWH_READING), 0), 3) AS tco2e_per_mwh
FROM actual_gen g
INNER JOIN unit_dim u ON u.DUID = g.DUID
GROUP BY u.REGIONID
ORDER BY tco2e_per_mwh ASC
LIMIT 1"""
        return QuerySpec(q, slug, "proxy", sql, note, "Lowest-emissions-intensity region")

    if q == "How does emissions intensity change by hour of day?":
        sql = f"""{with_ctes(ACTUAL_GEN_CTE, UNIT_DIM_CTE)}
SELECT
    toHour(g.INTERVAL_DATETIME) AS hour_of_day,
    round(sum(g.MWH_READING * coalesce(u.CO2E_EMISSIONS_FACTOR, 0)) / nullIf(sum(g.MWH_READING), 0), 3) AS tco2e_per_mwh
FROM actual_gen g
INNER JOIN unit_dim u ON u.DUID = g.DUID
GROUP BY hour_of_day
ORDER BY hour_of_day"""
        return QuerySpec(q, slug, "proxy", sql, note, "Emissions intensity by hour")

    if q == "What was the trend in emissions intensity month-over-month?":
        sql = f"""{with_ctes(ACTUAL_GEN_CTE, UNIT_DIM_CTE)}
SELECT
    toStartOfMonth(g.INTERVAL_DATETIME) AS month_start,
    round(sum(g.MWH_READING * coalesce(u.CO2E_EMISSIONS_FACTOR, 0)) / nullIf(sum(g.MWH_READING), 0), 3) AS tco2e_per_mwh
FROM actual_gen g
INNER JOIN unit_dim u ON u.DUID = g.DUID
GROUP BY month_start
ORDER BY month_start"""
        return QuerySpec(q, slug, "limited", sql, note, "Emissions intensity month-over-month")

    if q == "How much CO2 was avoided by renewable generation vs the marginal generator?":
        sql = diagnostic_sql(
            "Avoided CO2 versus the marginal generator needs marginal-unit identification and counterfactual dispatch logic."
        )
        return QuerySpec(q, slug, "blocked", sql, external_dataset_note("The semantic layer does not yet expose a defensible marginal-generator carbon counterfactual."), "Blocked")

    if q == "What is the minimum demand seen in each region and when does it typically occur?":
        sql = f"""{with_ctes(REGION_DISPATCH_CTE)}
SELECT
    REGIONID,
    min(TOTALDEMAND) AS minimum_demand_mw,
    argMin(SETTLEMENTDATE, TOTALDEMAND) AS occurred_at
FROM region_dispatch
GROUP BY REGIONID
ORDER BY minimum_demand_mw"""
        return QuerySpec(q, slug, "ready", sql, note, "Minimum demand by region")

    if q == "How much rooftop PV is estimated in each region?":
        sql = diagnostic_sql(
            "Rooftop PV needs an external rooftop PV or operational demand dataset."
        )
        return QuerySpec(q, slug, "blocked", sql, external_dataset_note("No rooftop PV dataset is currently loaded into the semantic layer."), "Blocked")

    if q == "What is the operational demand (grid demand minus rooftop PV) trend?":
        sql = diagnostic_sql(
            "Operational demand requires rooftop PV or an operational demand dataset."
        )
        return QuerySpec(q, slug, "blocked", sql, external_dataset_note("Operational demand is not derivable from the current semantic tables alone."), "Blocked")

    if q == "When does each region typically reach net zero or negative operational demand?":
        sql = diagnostic_sql(
            "Net operational demand requires rooftop PV or an operational demand dataset."
        )
        return QuerySpec(q, slug, "blocked", sql, external_dataset_note("The required operational-demand input is missing."), "Blocked")

    if q == "What is the overnight baseload demand by region?":
        sql = f"""{with_ctes(REGION_DISPATCH_CTE)}
SELECT
    REGIONID,
    round(avg(TOTALDEMAND), 2) AS overnight_baseload_mw
FROM region_dispatch
WHERE toHour(SETTLEMENTDATE) < 6
GROUP BY REGIONID
ORDER BY overnight_baseload_mw DESC"""
        return QuerySpec(q, slug, "ready", sql, note, "Overnight baseload demand")

    if q == "What was the total interconnector loss by flow direction this week?":
        sql = f"""{with_ctes(INTERCONNECTOR_CTE)}
SELECT
    INTERCONNECTORID,
    if(MWFLOW >= 0, 'Positive flow', 'Negative flow') AS flow_direction,
    round(sum(MWLOSSES / 12.0), 2) AS total_losses_mwh
FROM interconnector_dispatch
WHERE SETTLEMENTDATE >= date_trunc('week', now())
GROUP BY INTERCONNECTORID, flow_direction
ORDER BY INTERCONNECTORID, flow_direction"""
        return QuerySpec(q, slug, "proxy", sql, note, "Interconnector losses by flow direction")

    if q == "How do interconnector flows correlate with price differentials?":
        sql = f"""{with_ctes(
            INTERCONNECTOR_CTE,
            PRICE_DISPATCH_CTE,
            """
price_pairs AS (
    SELECT
        SETTLEMENTDATE,
        avgIf(RRP, REGIONID = 'NSW1') AS NSW1,
        avgIf(RRP, REGIONID = 'QLD1') AS QLD1,
        avgIf(RRP, REGIONID = 'VIC1') AS VIC1,
        avgIf(RRP, REGIONID = 'SA1') AS SA1,
        avgIf(RRP, REGIONID = 'TAS1') AS TAS1
    FROM price_dispatch
    GROUP BY SETTLEMENTDATE
)
"""
        )}
SELECT
    INTERCONNECTORID,
    corr(
        MWFLOW,
        CASE
            WHEN INTERCONNECTORID IN ('NSW1-QLD1', 'N-Q-MNSP1') THEN NSW1 - QLD1
            WHEN INTERCONNECTORID = 'VIC1-NSW1' THEN VIC1 - NSW1
            WHEN INTERCONNECTORID IN ('V-SA', 'V-S-MNSP1') THEN VIC1 - SA1
            WHEN INTERCONNECTORID = 'T-V-MNSP1' THEN TAS1 - VIC1
            ELSE NULL
        END
    ) AS flow_price_spread_correlation
FROM interconnector_dispatch i
LEFT JOIN price_pairs p ON p.SETTLEMENTDATE = i.SETTLEMENTDATE
GROUP BY INTERCONNECTORID
ORDER BY flow_price_spread_correlation DESC"""
        return QuerySpec(q, slug, "proxy", sql, note, "Interconnector flows versus price differentials")

    if q == "What is the marginal loss factor distribution across all DUIDs?":
        sql = """
SELECT
    DUID,
    TRANSMISSIONLOSSFACTOR AS MLF,
    REGIONID
FROM semantic.marginal_loss_factors
ORDER BY TRANSMISSIONLOSSFACTOR"""
        return QuerySpec(q, slug, "ready", sql, note, "Marginal loss factor distribution")

    if q == "How did actual interconnector flows compare to limits during peak demand?":
        sql = f"""{with_ctes(
            REGION_DISPATCH_CTE,
            INTERCONNECTOR_CTE,
            """
peak_intervals AS (
    SELECT argMax(SETTLEMENTDATE, TOTALDEMAND) AS peak_interval, REGIONID
    FROM region_dispatch
    GROUP BY REGIONID
)
"""
        )}
SELECT
    p.REGIONID,
    i.INTERCONNECTORID,
    i.SETTLEMENTDATE,
    round(i.MWFLOW, 2) AS flow_mw,
    round(i.EXPORTLIMIT, 2) AS export_limit_mw,
    round(i.IMPORTLIMIT, 2) AS import_limit_mw
FROM peak_intervals p
LEFT JOIN interconnector_dispatch i ON i.SETTLEMENTDATE = p.peak_interval
ORDER BY p.REGIONID, i.INTERCONNECTORID"""
        return QuerySpec(q, slug, "proxy", sql, note, "Interconnector flows at regional peak demand")

    if q == "What is the net export position of each region by hour?":
        sql = f"""{with_ctes(REGION_DISPATCH_CTE)}
SELECT
    toStartOfHour(SETTLEMENTDATE) AS hour_start,
    REGIONID,
    round(avg(NETINTERCHANGE), 2) AS avg_net_interchange_mw
FROM region_dispatch
GROUP BY hour_start, REGIONID
ORDER BY hour_start, REGIONID"""
        return QuerySpec(q, slug, "ready", sql, note, "Net export position by hour")

    if q == "How long did each generator run continuously in the last 7 days?":
        sql = diagnostic_sql(
            "Continuous run-length segmentation is not yet implemented in the report harness."
        )
        return QuerySpec(q, slug, "blocked", sql, external_dataset_note("The report harness does not yet segment continuous run blocks."), "Blocked")

    if q == "What was the capacity factor for each generator in the last 30 days?":
        sql = f"""{with_ctes(
            ACTUAL_GEN_CTE,
            UNIT_DIM_CTE,
            """
generator_totals AS (
    SELECT
        g.DUID,
        any(u.REGIONID) AS REGIONID,
        any(u.FUEL_TYPE) AS FUEL_TYPE,
        any(u.REGISTEREDCAPACITY_MW) AS registered_capacity_mw,
        sum(g.MWH_READING) AS energy_mwh,
        dateDiff('hour', min(g.INTERVAL_DATETIME), max(g.INTERVAL_DATETIME)) + 1 AS span_hours
    FROM actual_gen g
    INNER JOIN unit_dim u ON u.DUID = g.DUID
    WHERE g.INTERVAL_DATETIME >= now() - INTERVAL 30 DAY
      AND coalesce(u.DISPATCHTYPE, '') = 'GENERATOR'
    GROUP BY g.DUID
)
"""
        )}
SELECT
    DUID,
    REGIONID,
    FUEL_TYPE,
    round(energy_mwh / nullIf(registered_capacity_mw * span_hours, 0), 4) AS capacity_factor
FROM generator_totals
ORDER BY capacity_factor DESC"""
        return QuerySpec(q, slug, "limited", sql, note, "Generator capacity factors")

    if q == "Which generators had forced outages or sudden drops in output?":
        sql = f"""{with_ctes(ACTUAL_GEN_CTE, UNIT_DIM_CTE)}
SELECT
    DUID,
    any(REGIONID) AS REGIONID,
    any(FUEL_TYPE) AS FUEL_TYPE,
    min(delta_mwh) AS largest_interval_drop_mwh
FROM (
    SELECT
        g.INTERVAL_DATETIME,
        g.DUID,
        u.REGIONID,
        u.FUEL_TYPE,
        g.MWH_READING - lagInFrame(g.MWH_READING) OVER (
            PARTITION BY g.DUID ORDER BY g.INTERVAL_DATETIME
        ) AS delta_mwh
    FROM actual_gen g
    INNER JOIN unit_dim u ON u.DUID = g.DUID
)
GROUP BY DUID
ORDER BY largest_interval_drop_mwh ASC
LIMIT 25"""
        return QuerySpec(q, slug, "proxy", sql, note, "Largest sudden drops in output")

    if q == "How does actual output compare to registered capacity for intermittent generators?":
        sql = f"""{with_ctes(ACTUAL_GEN_CTE, UNIT_DIM_CTE)}
SELECT
    u.DUID,
    u.REGIONID,
    u.FUEL_TYPE,
    round(avg(g.MWH_READING * 12.0), 2) AS avg_output_mw,
    round(any(u.REGISTEREDCAPACITY_MW), 2) AS registered_capacity_mw,
    round(avg(g.MWH_READING * 12.0) / nullIf(any(u.REGISTEREDCAPACITY_MW), 0), 3) AS average_capacity_factor
FROM actual_gen g
INNER JOIN unit_dim u ON u.DUID = g.DUID
WHERE lower(u.FUEL_TYPE) IN ('solar', 'wind')
GROUP BY u.DUID, u.REGIONID, u.FUEL_TYPE
ORDER BY average_capacity_factor DESC"""
        return QuerySpec(q, slug, "ready", sql, note, "Actual output versus registered capacity for intermittent generators")

    if q == "What is the average time to ramp from zero to max for gas peakers?":
        sql = diagnostic_sql(
            "Ramp-to-max timing needs event segmentation from interval output traces."
        )
        return QuerySpec(q, slug, "blocked", sql, external_dataset_note("The report harness does not yet compute zero-to-max ramp episodes."), "Blocked")

    if q == "What is the total market value of energy traded in each region per day?":
        sql = f"""{with_ctes(REGION_DISPATCH_CTE)}
SELECT
    toDate(SETTLEMENTDATE) AS trading_day,
    REGIONID,
    round(sum((TOTALDEMAND / 12.0) * RRP), 2) AS market_value_proxy
FROM region_dispatch
GROUP BY trading_day, REGIONID
ORDER BY trading_day, REGIONID"""
        return QuerySpec(q, slug, "proxy", sql, note, "Daily market value by region")

    if q == "What was the weighted average price paid by load in each region?":
        sql = f"""{with_ctes(REGION_DISPATCH_CTE)}
SELECT
    REGIONID,
    round(sum((TOTALDEMAND / 12.0) * RRP) / nullIf(sum(TOTALDEMAND / 12.0), 0), 2) AS demand_weighted_price
FROM region_dispatch
GROUP BY REGIONID
ORDER BY demand_weighted_price DESC"""
        return QuerySpec(q, slug, "ready", sql, note, "Demand-weighted price by region")

    if q == "How does the wholesale cost of electricity break down by component (energy, FCAS, losses)?":
        sql = diagnostic_sql(
            "A component breakdown needs an explicit settlement mart for energy, FCAS, and losses."
        )
        return QuerySpec(q, slug, "blocked", sql, external_dataset_note("The semantic layer does not yet expose settlement-grade cost components."), "Blocked")

    if q == "What is the average daily settlement amount by region?":
        sql = f"""{with_ctes(
            REGION_DISPATCH_CTE,
            """
daily_value AS (
    SELECT
        toDate(SETTLEMENTDATE) AS trading_day,
        REGIONID,
        sum((TOTALDEMAND / 12.0) * RRP) AS market_value_proxy
    FROM region_dispatch
    GROUP BY trading_day, REGIONID
)
"""
        )}
SELECT REGIONID, round(avg(market_value_proxy), 2) AS avg_daily_settlement_proxy
FROM daily_value
GROUP BY REGIONID
ORDER BY avg_daily_settlement_proxy DESC"""
        return QuerySpec(q, slug, "proxy", sql, note, "Average daily settlement proxy")

    if q == "How volatile is the daily market value compared to historical averages?":
        sql = f"""{with_ctes(
            REGION_DISPATCH_CTE,
            """
daily_value AS (
    SELECT
        toDate(SETTLEMENTDATE) AS trading_day,
        REGIONID,
        sum((TOTALDEMAND / 12.0) * RRP) AS market_value_proxy
    FROM region_dispatch
    GROUP BY trading_day, REGIONID
)
"""
        )}
SELECT
    REGIONID,
    round(avg(market_value_proxy), 2) AS avg_daily_value,
    round(stddevPop(market_value_proxy), 2) AS volatility_against_available_history
FROM daily_value
GROUP BY REGIONID
ORDER BY volatility_against_available_history DESC"""
        return QuerySpec(q, slug, "limited", sql, note, "Daily market value volatility")

    if q == "What is the daily demand-weighted price by region?":
        sql = f"""{with_ctes(REGION_DISPATCH_CTE)}
SELECT
    toDate(SETTLEMENTDATE) AS trading_day,
    REGIONID,
    round(sum((TOTALDEMAND / 12.0) * RRP) / nullIf(sum(TOTALDEMAND / 12.0), 0), 2) AS demand_weighted_price
FROM region_dispatch
GROUP BY trading_day, REGIONID
ORDER BY trading_day, REGIONID"""
        return QuerySpec(q, slug, "ready", sql, note, "Daily demand-weighted price")

    if q == "How does the time-weighted average price compare to the demand-weighted average price?":
        sql = f"""{with_ctes(REGION_DISPATCH_CTE)}
SELECT
    REGIONID,
    round(avg(RRP), 2) AS time_weighted_price,
    round(sum((TOTALDEMAND / 12.0) * RRP) / nullIf(sum(TOTALDEMAND / 12.0), 0), 2) AS demand_weighted_price
FROM region_dispatch
GROUP BY REGIONID
ORDER BY REGIONID"""
        return QuerySpec(q, slug, "ready", sql, note, "Time-weighted versus demand-weighted price")

    if q == "What is the implied value of storage for 2-hour and 4-hour batteries at current spreads?":
        sql = f"""{with_ctes(
            PRICE_DISPATCH_CTE,
            """
daily_spreads AS (
    SELECT
        toDate(SETTLEMENTDATE) AS trading_day,
        REGIONID,
        max(RRP) - min(RRP) AS intraday_spread
    FROM price_dispatch
    GROUP BY trading_day, REGIONID
)
"""
        )}
SELECT
    trading_day,
    REGIONID,
    round(intraday_spread * 2, 2) AS implied_value_2h_mwday,
    round(intraday_spread * 4, 2) AS implied_value_4h_mwday
FROM daily_spreads
ORDER BY trading_day, REGIONID"""
        return QuerySpec(q, slug, "proxy", sql, note, "Implied storage value from intraday spreads")

    if q == "What was the cost of unserved energy by region?":
        sql = diagnostic_sql(
            "Unserved energy cost needs a USE or involuntary load shedding dataset."
        )
        return QuerySpec(q, slug, "blocked", sql, external_dataset_note("No unserved-energy dataset is available in the semantic layer."), "Blocked")

    if q == "Which region had the most volatile intraday price swings?":
        sql = f"""{with_ctes(
            PRICE_DISPATCH_CTE,
            """
daily_swings AS (
    SELECT
        toDate(SETTLEMENTDATE) AS trading_day,
        REGIONID,
        max(RRP) - min(RRP) AS intraday_swing
    FROM price_dispatch
    GROUP BY trading_day, REGIONID
)
"""
        )}
SELECT
    REGIONID,
    round(avg(intraday_swing), 2) AS avg_intraday_swing,
    round(max(intraday_swing), 2) AS max_intraday_swing
FROM daily_swings
GROUP BY REGIONID
ORDER BY avg_intraday_swing DESC"""
        return QuerySpec(q, slug, "ready", sql, note, "Intraday price swing volatility")

    if q == "How many unique DUIDs were dispatched in each region yesterday?":
        sql = f"""{with_ctes(UNIT_DISPATCH_CTE, UNIT_DIM_CTE)}
SELECT
    coalesce(u.REGIONID, 'UNKNOWN') AS REGIONID,
    countDistinct(d.DUID) AS dispatched_duids
FROM unit_dispatch d
LEFT JOIN unit_dim u ON u.DUID = d.DUID
WHERE toDate(d.SETTLEMENTDATE) = yesterday()
  AND abs(d.TOTALCLEARED) > 0.01
GROUP BY REGIONID
ORDER BY dispatched_duids DESC"""
        return QuerySpec(q, slug, "ready", sql, note, "Unique dispatched DUIDs by region")

    if q == "What is the average number of generators online by hour of day?":
        sql = f"""{with_ctes(
            ACTUAL_GEN_CTE,
            UNIT_DIM_CTE,
            """
hourly_counts AS (
    SELECT
        toStartOfHour(g.INTERVAL_DATETIME) AS hour_start,
        countDistinctIf(g.DUID, g.MWH_READING > 0 AND coalesce(u.DISPATCHTYPE, '') = 'GENERATOR') AS generators_online
    FROM actual_gen g
    LEFT JOIN unit_dim u ON u.DUID = g.DUID
    GROUP BY hour_start
)
"""
        )}
SELECT
    toHour(hour_start) AS hour_of_day,
    round(avg(generators_online), 2) AS avg_generators_online
FROM hourly_counts
GROUP BY hour_of_day
ORDER BY hour_of_day"""
        return QuerySpec(q, slug, "ready", sql, note, "Average generators online by hour")

    if q == "Which semi-scheduled generators had the most curtailment?":
        sql = f"""{with_ctes(UNIT_SOLUTION_CTE, UNIT_DIM_CTE)}
SELECT
    s.DUID,
    any(u.REGIONID) AS REGIONID,
    any(u.FUEL_TYPE) AS FUEL_TYPE,
    round(sum(greatest(s.UIGF - s.TOTALCLEARED, 0) / 12.0), 2) AS curtailed_mwh_proxy
FROM unit_solution s
INNER JOIN unit_dim u ON u.DUID = s.DUID
WHERE coalesce(u.SCHEDULE_TYPE, '') = 'SEMI-SCHEDULED'
GROUP BY s.DUID
ORDER BY curtailed_mwh_proxy DESC
LIMIT 25"""
        return QuerySpec(q, slug, "proxy", sql, note, "Semi-scheduled curtailment proxy")

    if q == "What is the correlation between gas prices and NEM electricity prices?":
        sql = diagnostic_sql(
            "Gas price correlation requires an external gas market dataset."
        )
        return QuerySpec(q, slug, "blocked", sql, external_dataset_note("No gas price dataset is loaded into ClickHouse."), "Blocked")

    if q == "How does the NEM compare across regions in terms of price, demand, and generation mix?":
        sql = f"""{with_ctes(
            REGION_DISPATCH_CTE,
            ACTUAL_GEN_CTE,
            UNIT_DIM_CTE,
            f"""
gen_mix AS (
    SELECT
        u.REGIONID,
        {RENEWABLE_CASE} AS generation_group,
        sum(g.MWH_READING) AS energy_mwh
    FROM actual_gen g
    INNER JOIN unit_dim u ON u.DUID = g.DUID
    GROUP BY u.REGIONID, generation_group
)
"""
        )}
SELECT
    d.REGIONID,
    round(avg(d.RRP), 2) AS avg_price_mwh,
    round(avg(d.TOTALDEMAND), 2) AS avg_demand_mw,
    round(maxIf(g.energy_mwh, g.generation_group = 'Renewables') / nullIf(sum(g.energy_mwh), 0), 4) AS renewable_share
FROM region_dispatch d
LEFT JOIN gen_mix g ON g.REGIONID = d.REGIONID
GROUP BY d.REGIONID
ORDER BY avg_price_mwh DESC"""
        return QuerySpec(q, slug, "proxy", sql, note, "Regional comparison across price, demand, and generation mix")

    if q == "What is the all-time minimum demand record for each region?":
        sql = f"""{with_ctes(REGION_DISPATCH_CTE)}
SELECT
    REGIONID,
    min(TOTALDEMAND) AS minimum_demand_mw,
    argMin(SETTLEMENTDATE, TOTALDEMAND) AS occurred_at
FROM region_dispatch
GROUP BY REGIONID
ORDER BY minimum_demand_mw"""
        return QuerySpec(q, slug, "limited", sql, note, "All-time minimum demand over available history")

    if q == "What is the all-time maximum price record for each region and when did it occur?":
        sql = f"""{with_ctes(PRICE_DISPATCH_CTE)}
SELECT
    REGIONID,
    max(RRP) AS maximum_spot_price,
    argMax(SETTLEMENTDATE, RRP) AS occurred_at
FROM price_dispatch
GROUP BY REGIONID
ORDER BY maximum_spot_price DESC"""
        return QuerySpec(q, slug, "limited", sql, note, "All-time maximum price over available history")

    if q == "How has the number of registered DUIDs changed over the last 5 years?":
        sql = """
SELECT
    toStartOfYear(START_DATE) AS year_start,
    countDistinct(DUID) AS registered_duids
FROM semantic.participant_registration_dudetailsummary
WHERE START_DATE >= now() - INTERVAL 5 YEAR
GROUP BY year_start
ORDER BY year_start"""
        return QuerySpec(q, slug, "ready", sql, note, "Registered DUID count over five years")

    if q == "What is the seasonal demand pattern for each region?":
        sql = f"""{with_ctes(REGION_DISPATCH_CTE)}
SELECT
    REGIONID,
    CASE
        WHEN toMonth(SETTLEMENTDATE) IN (12, 1, 2) THEN 'Summer'
        WHEN toMonth(SETTLEMENTDATE) IN (3, 4, 5) THEN 'Autumn'
        WHEN toMonth(SETTLEMENTDATE) IN (6, 7, 8) THEN 'Winter'
        ELSE 'Spring'
    END AS season,
    round(avg(TOTALDEMAND), 2) AS avg_demand_mw
FROM region_dispatch
GROUP BY REGIONID, season
ORDER BY REGIONID, season"""
        return QuerySpec(q, slug, "limited", sql, note, "Seasonal demand pattern")

    if q == "How does summer peak demand compare to winter peak demand?":
        sql = f"""{with_ctes(REGION_DISPATCH_CTE)}
SELECT
    REGIONID,
    season,
    max(TOTALDEMAND) AS peak_demand_mw
FROM (
    SELECT
        REGIONID,
        TOTALDEMAND,
        CASE
            WHEN toMonth(SETTLEMENTDATE) IN (12, 1, 2) THEN 'Summer'
            WHEN toMonth(SETTLEMENTDATE) IN (6, 7, 8) THEN 'Winter'
            ELSE 'Other'
        END AS season
    FROM region_dispatch
)
WHERE season IN ('Summer', 'Winter')
GROUP BY REGIONID, season
ORDER BY REGIONID, season"""
        return QuerySpec(q, slug, "limited", sql, note, "Summer versus winter peak demand")

    if q == "What is the daytime (9am-5pm) vs nighttime (5pm-9am) average price by region?":
        sql = f"""{with_ctes(PRICE_DISPATCH_CTE)}
SELECT
    REGIONID,
    if(toHour(SETTLEMENTDATE) BETWEEN 9 AND 16, 'Daytime', 'Nighttime') AS period_bucket,
    round(avg(RRP), 2) AS avg_price_mwh
FROM price_dispatch
GROUP BY REGIONID, period_bucket
ORDER BY REGIONID, period_bucket"""
        return QuerySpec(q, slug, "ready", sql, note, "Daytime versus nighttime average price")

    if q == "How does the price volatility on weekdays compare to weekends?":
        sql = f"""{with_ctes(PRICE_DISPATCH_CTE)}
SELECT
    REGIONID,
    if(toDayOfWeek(SETTLEMENTDATE) IN (6, 7), 'Weekend', 'Weekday') AS day_type,
    round(stddevPop(RRP), 2) AS price_volatility
FROM price_dispatch
GROUP BY REGIONID, day_type
ORDER BY REGIONID, day_type"""
        return QuerySpec(q, slug, "ready", sql, note, "Weekday versus weekend price volatility")

    if q == "What is the correlation between temperature and demand by region?":
        sql = diagnostic_sql(
            "Temperature correlation requires an external weather dataset."
        )
        return QuerySpec(q, slug, "blocked", sql, external_dataset_note("No temperature dataset is loaded into ClickHouse."), "Blocked")

    if q == "Which hours have the highest average prices by region?":
        sql = f"""{with_ctes(PRICE_DISPATCH_CTE)}
SELECT
    REGIONID,
    toHour(SETTLEMENTDATE) AS hour_of_day,
    round(avg(RRP), 2) AS avg_price_mwh
FROM price_dispatch
GROUP BY REGIONID, hour_of_day
ORDER BY REGIONID, avg_price_mwh DESC"""
        return QuerySpec(q, slug, "ready", sql, note, "Highest-average-price hours by region")

    if q == "What is the rolling 7-day average price trend for each region?":
        sql = f"""{with_ctes(
            PRICE_DISPATCH_CTE,
            """
daily_price AS (
    SELECT
        toDate(SETTLEMENTDATE) AS trading_day,
        REGIONID,
        avg(RRP) AS avg_daily_price
    FROM price_dispatch
    GROUP BY trading_day, REGIONID
)
"""
        )}
SELECT
    trading_day,
    REGIONID,
    round(avg_daily_price, 2) AS avg_daily_price,
    round(avg(avg_daily_price) OVER (
        PARTITION BY REGIONID ORDER BY trading_day
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2) AS rolling_7d_avg_price
FROM daily_price
ORDER BY trading_day, REGIONID"""
        return QuerySpec(q, slug, "ready", sql, note, "Rolling 7-day average price trend")

    if q == "How many hours per week does each region spend above $100/MWh?":
        sql = f"""{with_ctes(
            PRICE_DISPATCH_CTE,
            """
hourly_price AS (
    SELECT
        toStartOfHour(SETTLEMENTDATE) AS hour_start,
        REGIONID,
        avg(RRP) AS hourly_avg_price
    FROM price_dispatch
    GROUP BY hour_start, REGIONID
)
"""
        )}
SELECT
    REGIONID,
    toStartOfWeek(hour_start) AS week_start,
    countIf(hourly_avg_price > 100) AS hours_above_100
FROM hourly_price
GROUP BY REGIONID, week_start
ORDER BY week_start, REGIONID"""
        return QuerySpec(q, slug, "ready", sql, note, "Hours above $100/MWh by week")

    if q == "What is the average ramp rate required from dispatchable generators by hour?":
        sql = f"""{with_ctes(
            UNIT_DISPATCH_CTE,
            UNIT_DIM_CTE,
            """
hourly_ramp AS (
    SELECT
        d.SETTLEMENTDATE,
        toHour(d.SETTLEMENTDATE) AS hour_of_day,
        d.DUID,
        abs(d.TOTALCLEARED - lagInFrame(d.TOTALCLEARED) OVER (
            PARTITION BY d.DUID ORDER BY d.SETTLEMENTDATE
        )) * 12.0 AS ramp_rate_mw_per_hour
    FROM unit_dispatch d
    INNER JOIN unit_dim u ON u.DUID = d.DUID
    WHERE lower(u.FUEL_TYPE) NOT IN ('solar', 'wind')
)
"""
        )}
SELECT
    hour_of_day,
    round(avg(ramp_rate_mw_per_hour), 2) AS avg_ramp_rate_mw_per_hour
FROM hourly_ramp
GROUP BY hour_of_day
ORDER BY hour_of_day"""
        return QuerySpec(q, slug, "proxy", sql, note, "Average ramp rate by hour")

    if q == "How much frequency regulation FCAS is typically enabled by region?":
        sql = f"""{with_ctes(REGION_DISPATCH_CTE)}
SELECT
    REGIONID,
    round(avg(RAISEREGLOCALDISPATCH), 2) AS avg_raise_reg_mw,
    round(avg(LOWERREGLOCALDISPATCH), 2) AS avg_lower_reg_mw
FROM region_dispatch
GROUP BY REGIONID
ORDER BY REGIONID"""
        return QuerySpec(q, slug, "proxy", sql, note, "Typical frequency-regulation FCAS enablement")

    if q == "What is the distribution of 5-minute price outcomes (histogram) by region?":
        sql = f"""{with_ctes(PRICE_DISPATCH_CTE)}
SELECT
    REGIONID,
    widthBucket(RRP, -1000, 2000, 20) AS price_bucket,
    count() AS interval_count
FROM price_dispatch
GROUP BY REGIONID, price_bucket
ORDER BY REGIONID, price_bucket"""
        return QuerySpec(q, slug, "ready", sql, note, "Price outcome histogram by region")

    if q == "What share of demand is met by local generation vs imports in each region?":
        sql = f"""{with_ctes(REGION_DISPATCH_CTE)}
SELECT
    REGIONID,
    round(avg(TOTALDEMAND - abs(NETINTERCHANGE)), 2) AS local_generation_proxy_mw,
    round(avg(abs(NETINTERCHANGE)), 2) AS imports_proxy_mw,
    round(avg(TOTALDEMAND - abs(NETINTERCHANGE)) / nullIf(avg(TOTALDEMAND), 0), 4) AS local_share_proxy
FROM region_dispatch
GROUP BY REGIONID
ORDER BY local_share_proxy DESC"""
        proxy_note = (
            "This approximates local generation share from total demand and absolute net interchange. "
            "It is direction-agnostic and should be treated as a rough operational proxy. "
            + note
        )
        return QuerySpec(q, slug, "proxy", sql, proxy_note, "Local generation versus imports proxy")

    if q == "How does the SA generation mix differ from NSW?":
        sql = f"""{with_ctes(ACTUAL_GEN_CTE, UNIT_DIM_CTE)}
SELECT
    u.REGIONID,
    u.FUEL_TYPE,
    round(sum(g.MWH_READING), 2) AS generation_mwh
FROM actual_gen g
INNER JOIN unit_dim u ON u.DUID = g.DUID
WHERE u.REGIONID IN ('SA1', 'NSW1')
GROUP BY u.REGIONID, u.FUEL_TYPE
ORDER BY u.REGIONID, generation_mwh DESC"""
        return QuerySpec(q, slug, "ready", sql, note, "SA versus NSW generation mix")

    if q == "Which region is most dependent on interconnector imports?":
        sql = f"""{with_ctes(REGION_DISPATCH_CTE)}
SELECT
    REGIONID,
    round(avg(abs(NETINTERCHANGE)) / nullIf(avg(TOTALDEMAND), 0), 4) AS import_dependency_proxy
FROM region_dispatch
GROUP BY REGIONID
ORDER BY import_dependency_proxy DESC"""
        return QuerySpec(q, slug, "proxy", sql, note, "Regional import dependency proxy")

    if q == "How many MW of gas generation capacity is registered but rarely dispatched?":
        sql = f"""{with_ctes(
            ACTUAL_GEN_CTE,
            UNIT_DIM_CTE,
            """
gas_units AS (
    SELECT
        u.DUID,
        u.REGIONID,
        u.REGISTEREDCAPACITY_MW,
        sum(g.MWH_READING) AS energy_mwh,
        dateDiff('hour', min(g.INTERVAL_DATETIME), max(g.INTERVAL_DATETIME)) + 1 AS span_hours
    FROM unit_dim u
    LEFT JOIN actual_gen g ON g.DUID = u.DUID
    WHERE lower(u.FUEL_TYPE) = 'natural gas (pipeline)'
    GROUP BY u.DUID, u.REGIONID, u.REGISTEREDCAPACITY_MW
)
"""
        )}
SELECT
    REGIONID,
    round(sumIf(REGISTEREDCAPACITY_MW, energy_mwh / nullIf(REGISTEREDCAPACITY_MW * span_hours, 0) < 0.1), 2) AS rarely_dispatched_capacity_mw
FROM gas_units
GROUP BY REGIONID
ORDER BY rarely_dispatched_capacity_mw DESC"""
        return QuerySpec(q, slug, "proxy", sql, note, "Rarely dispatched gas capacity")

    if q == "What is the current headroom (spare capacity) in each region at peak?":
        sql = f"""{with_ctes(
            REGION_DISPATCH_CTE,
            """
peak_intervals AS (
    SELECT
        REGIONID,
        argMax(SETTLEMENTDATE, TOTALDEMAND) AS peak_interval
    FROM region_dispatch
    GROUP BY REGIONID
)
"""
        )}
SELECT
    p.REGIONID,
    r.SETTLEMENTDATE,
    round(r.AVAILABLEGENERATION - r.TOTALDEMAND, 2) AS spare_capacity_mw
FROM peak_intervals p
LEFT JOIN region_dispatch r
    ON r.REGIONID = p.REGIONID
   AND r.SETTLEMENTDATE = p.peak_interval
ORDER BY spare_capacity_mw ASC"""
        return QuerySpec(q, slug, "proxy", sql, note, "Spare capacity at regional peak")

    if "temperature" in q.lower():
        return QuerySpec(q, slug, "blocked", diagnostic_sql("No temperature dataset is loaded into ClickHouse."), external_dataset_note("Temperature-driven analysis is blocked by missing weather data."), "Blocked")
    if "gas prices" in q.lower():
        return QuerySpec(q, slug, "blocked", diagnostic_sql("No gas price dataset is loaded into ClickHouse."), external_dataset_note("Gas-price correlation is blocked by missing gas data."), "Blocked")
    if "rooftop pv" in q.lower() or "operational demand" in q.lower() or "net zero or negative operational demand" in q.lower():
        return QuerySpec(q, slug, "blocked", diagnostic_sql("No rooftop PV dataset is loaded into ClickHouse."), external_dataset_note("Rooftop PV and operational demand analysis is blocked by missing inputs."), "Blocked")
    if "settlement amount" in q.lower() or "settlement-grade" in q.lower():
        return QuerySpec(q, slug, "blocked", diagnostic_sql("Settlement-grade marts are not available."), external_dataset_note("Exact settlement analysis is blocked by the absence of settlement marts."), "Blocked")

    return QuerySpec(
        q,
        slug,
        "blocked",
        diagnostic_sql("No explicit report mapping has been implemented for this question yet."),
        "The generator produced a placeholder report because this question has not been wired to a semantic SQL template yet. "
        + note,
        "Blocked",
    )


def build_chart(df: pd.DataFrame, question: str, out_path: Path, status: str) -> None:
    plt.style.use("ggplot")
    fig, ax = plt.subplots(figsize=(10, 5.5))
    if status == "blocked":
        ax.text(0.5, 0.5, "Blocked or not yet implemented", ha="center", va="center", fontsize=18)
        ax.axis("off")
        fig.suptitle(question, fontsize=12)
        fig.tight_layout()
        fig.savefig(out_path, dpi=150)
        plt.close(fig)
        return

    if df.empty:
        ax.text(0.5, 0.5, "Query returned no rows", ha="center", va="center", fontsize=18)
        ax.axis("off")
        fig.suptitle(question, fontsize=12)
        fig.tight_layout()
        fig.savefig(out_path, dpi=150)
        plt.close(fig)
        return

    dt_cols = [c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c])]
    numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    category_cols = [c for c in df.columns if c not in dt_cols and c not in numeric_cols]

    if dt_cols and numeric_cols:
        x_col = dt_cols[0]
        y_col = numeric_cols[0]
        if category_cols:
            cat_col = category_cols[0]
            for key, sub in list(df.groupby(cat_col))[:6]:
                sub = sub.sort_values(x_col)
                ax.plot(sub[x_col], sub[y_col], label=str(key))
            ax.legend(loc="best", fontsize=8)
        else:
            sub = df.sort_values(x_col)
            ax.plot(sub[x_col], sub[y_col], linewidth=2)
        ax.set_xlabel(x_col)
        ax.set_ylabel(y_col)
    elif category_cols and numeric_cols:
        x_col = category_cols[0]
        y_col = numeric_cols[0]
        sub = df.sort_values(y_col, ascending=False).head(20)
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

    fig.suptitle(question, fontsize=12)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def summarize(df: pd.DataFrame, spec: QuerySpec) -> str:
    if spec.status == "blocked":
        return "The question is currently blocked because the required data product or modeled analytic surface is not available."
    if df.empty:
        return "The query ran successfully but returned no rows."

    numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    lines = []
    if numeric_cols:
        first_numeric = numeric_cols[0]
        top = df.iloc[0]
        top_parts = []
        for col in df.columns[: min(4, len(df.columns))]:
            top_parts.append(f"{col}={top[col]}")
        lines.append("Top row: " + ", ".join(top_parts) + ".")
        if len(df) > 1:
            lines.append(
                f"Returned {len(df):,} rows. The mean of `{first_numeric}` across the result set is "
                f"{pd.to_numeric(df[first_numeric], errors='coerce').dropna().mean():.3f}."
            )
    else:
        lines.append(f"Returned {len(df):,} rows.")
    return " ".join(lines)


def write_markdown(spec: QuerySpec, summary: str, target_dir: Path, chart_name: str) -> None:
    md = textwrap.dedent(
        f"""\
        # {spec.question}

        - Status: `{spec.status}`
        - Chart: `{chart_name}`
        - Raw results: `results.csv`

        ## Data

        {spec.data_description}

        ## Note

        {spec.note}

        ## Explanation

        {summary}

        ## SQL

        ```sql
        {spec.sql}
        ```
        """
    )
    (target_dir / "report.md").write_text(md)


def write_pdf(spec: QuerySpec, summary: str, df: pd.DataFrame, target_dir: Path, chart_path: Path) -> None:
    doc = SimpleDocTemplate(str(target_dir / "report.pdf"), pagesize=A4, rightMargin=36, leftMargin=36, topMargin=36, bottomMargin=36)
    styles = getSampleStyleSheet()
    mono = ParagraphStyle(
        "MonoSmall",
        parent=styles["Code"],
        fontName="Courier",
        fontSize=7.5,
        leading=9,
    )
    elements = [
        Paragraph(spec.question, styles["Title"]),
        Spacer(1, 8),
        Paragraph(f"Status: <b>{spec.status}</b>", styles["BodyText"]),
        Spacer(1, 8),
        Paragraph("Data", styles["Heading2"]),
        Paragraph(spec.data_description, styles["BodyText"]),
        Spacer(1, 8),
        Paragraph("Note", styles["Heading2"]),
        Paragraph(spec.note, styles["BodyText"]),
        Spacer(1, 8),
        Paragraph("Explanation", styles["Heading2"]),
        Paragraph(summary, styles["BodyText"]),
        Spacer(1, 12),
    ]
    if chart_path.exists():
        elements.append(Image(str(chart_path), width=6.9 * inch, height=3.8 * inch))
        elements.append(Spacer(1, 12))

    preview = df.head(12).fillna("").astype(str)
    if not preview.empty:
        table_data = [list(preview.columns)] + preview.values.tolist()
        table = Table(table_data, repeatRows=1)
        table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#dce6f1")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.black),
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
    elements.append(Preformatted(spec.sql, mono))
    doc.build(elements)


def write_raw_files(spec: QuerySpec, df: pd.DataFrame, target_dir: Path) -> None:
    (target_dir / "query.sql").write_text(spec.sql)
    df.to_csv(target_dir / "results.csv", index=False)
    (target_dir / "spec.json").write_text(
        json.dumps(
            {
                "question": spec.question,
                "slug": spec.slug,
                "status": spec.status,
                "data_description": spec.data_description,
                "note": spec.note,
            },
            indent=2,
        )
    )


def render_question(client, question: str, output_root: Path) -> dict:
    spec = infer_question(question)
    spec.data_description = infer_data_description(spec.sql, spec.status)
    target_dir = output_root / spec.slug
    target_dir.mkdir(parents=True, exist_ok=True)

    try:
        df = query_dataframe(client, spec.sql)
        if not df.empty:
            for col in df.columns:
                if "date" in col.lower() or "time" in col.lower():
                    converted = pd.to_datetime(df[col], errors="ignore")
                    df[col] = converted
        summary = summarize(df, spec)
        error_text = None
    except Exception as exc:
        df = pd.DataFrame({"error": [str(exc)]})
        summary = f"SQL execution failed: {exc}"
        error_text = str(exc)

    chart_path = target_dir / "chart.png"
    build_chart(df, spec.chart_title, chart_path, "blocked" if error_text else spec.status)
    write_raw_files(spec, df, target_dir)
    write_markdown(spec, summary, target_dir, chart_path.name)
    write_pdf(spec, summary, df, target_dir, chart_path)
    return {
        "question": spec.question,
        "slug": spec.slug,
        "status": spec.status if not error_text else "error",
        "row_count": int(len(df)),
        "error": error_text,
    }


def load_questions(path: Path) -> list[str]:
    return [line.strip() for line in path.read_text().splitlines() if line.strip()]


def write_index(records: Iterable[dict], output_root: Path) -> None:
    rows = list(records)
    df = pd.DataFrame(rows)
    df.to_csv(output_root / "index.csv", index=False)
    lines = ["# NEM Question Reports", ""]
    for record in rows:
        lines.append(
            f"- `{record['status']}` [{record['slug']}](./{record['slug']}/report.md): {record['question']}"
        )
    (output_root / "README.md").write_text("\n".join(lines) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate per-question NEM report bundles")
    parser.add_argument("--questions", default=str(QUESTION_FILE))
    parser.add_argument("--output", default=str(OUTPUT_ROOT))
    args = parser.parse_args()

    output_root = Path(args.output)
    output_root.mkdir(parents=True, exist_ok=True)
    client = connect()
    questions = load_questions(Path(args.questions))
    records = [render_question(client, question, output_root) for question in questions]
    write_index(records, output_root)
    status_counts = pd.Series([record["status"] for record in records]).value_counts().to_dict()
    print(json.dumps({"output_root": str(output_root), "status_counts": status_counts}, indent=2))


if __name__ == "__main__":
    main()
