# Australian Energy Market Questions

This note is grounded in the current ClickHouse state on March 12, 2026.

## Semantic Views (use these, not raw tables)

The `semantic` database provides clean, stable views over the hash-specific raw tables:

### Core market data
- `semantic.dispatch_price` - 5-min spot prices + FCAS prices by region
- `semantic.dispatch_regionsum` - 5-min regional demand, generation, FCAS volumes
- `semantic.dispatch_unit_scada` - 5-min SCADA MW output per DUID
- `semantic.dispatch_interconnectorres` - 5-min interconnector flows
- `semantic.dispatch_constraint` - binding constraint details
- `semantic.dispatch_case_solution` - dispatch engine solution metadata
- `semantic.dispatch_local_price` - connection point local prices
- `semantic.dispatch_interconnection` - interconnection summary
- `semantic.dispatch_regionfcas` - FCAS requirements by region
- `semantic.dispatch_irsr` - inter-regional settlements residues

### Generation and metering
- `semantic.actual_gen_duid` - next-day actual metered generation by DUID
- `semantic.daily_unit_dispatch` - daily unit dispatch summary
- `semantic.daily_region_dispatch` - daily regional dispatch summary
- `semantic.intermittent_gen_scada` - intermittent generator SCADA
- `semantic.demand_historic` - historical demand by region

### Bids and offers
- `semantic.bid_dayoffer` - daily bid offers
- `semantic.bid_peroffer` - per-period bid offers (bid stack)
- `semantic.next_day_energy_bids` - next-day energy bids (sparse format)

### Reference and forecasting
- `semantic.marginal_loss_factors` - MLF by DUID
- `semantic.mtpasa_duid_availability` - medium-term DUID availability
- `semantic.fpp_contribution_factor` - FCAS causer pays contribution

### Metadata and discovery
- `semantic.table_locator` - maps logical table names to physical tables
- `semantic.schema_registry` - all observed schemas
- `semantic.data_availability` - what data is loaded and how much
- `semantic.mms_tables` - MMS Data Model table descriptions
- `semantic.mms_columns` - MMS Data Model column descriptions
- `semantic.table_descriptions` - human-readable table explanations
- `semantic.column_descriptions` - human-readable column explanations
- `semantic.population_dates` - when each table started receiving data

### Important constraints

- Registration tables (DUDETAILSUMMARY, GENUNITS, etc.) are still being ingested via MMSDM. Once available, region/fuel/station/participant joins will work.
- Exact settlement revenue is not public. `SET_ENERGY_GENSET_DETAIL` and `SETGENDATA` are marked `Private` by AEMO. Revenue can only be approximated from dispatch * price.

## Questions That Work Now

### 1. What was the average spot price in each region over the last 7 days?

Status: works now

```sql
SELECT
  toDate(SETTLEMENTDATE) AS day,
  REGIONID,
  round(avg(RRP), 2) AS avg_rrp
FROM semantic.dispatch_price
WHERE SETTLEMENTDATE >= now() - INTERVAL 7 DAY
GROUP BY day, REGIONID
ORDER BY day DESC, REGIONID;
```

### 2. Which region had the biggest demand forecast error each day for the last 30 days?

Status: works now

```sql
WITH daily_error AS (
  SELECT
    toDate(SETTLEMENTDATE) AS day,
    REGIONID,
    avg(abs(TOTALDEMAND - DEMANDFORECAST)) AS mae_mw
  FROM semantic.dispatch_price
  WHERE SETTLEMENTDATE >= now() - INTERVAL 30 DAY
  GROUP BY day, REGIONID
)
SELECT
  day,
  REGIONID AS worst_region,
  round(mae_mw, 2) AS mae_mw
FROM (
  SELECT
    day,
    REGIONID,
    mae_mw,
    row_number() OVER (PARTITION BY day ORDER BY mae_mw DESC) AS rn
  FROM daily_error
)
WHERE rn = 1
ORDER BY day DESC;
```

### 3. How volatile were 5-minute prices by region over the last 7 days?

Status: works now

```sql
SELECT
  REGIONID,
  round(stddevSamp(RRP), 2) AS rrp_volatility,
  round(avg(RRP), 2) AS avg_rrp
FROM semantic.dispatch_price
WHERE SETTLEMENTDATE >= now() - INTERVAL 7 DAY
GROUP BY REGIONID
ORDER BY rrp_volatility DESC;
```

### 4. What was daily regional demand over the last 7 days?

Status: works now

Note: `TOTALDEMAND` is MW at 5-minute resolution. Dividing the sum by 12 gives an approximate MWh total for the day.

```sql
SELECT
  toDate(SETTLEMENTDATE) AS day,
  REGIONID,
  round(sum(TOTALDEMAND) / 12, 1) AS approx_daily_mwh_demand
FROM semantic.dispatch_price
WHERE SETTLEMENTDATE >= now() - INTERVAL 7 DAY
GROUP BY day, REGIONID
ORDER BY day DESC, REGIONID;
```

### 5. How wide was the NSW-VIC price spread each day over the last week?

Status: works now

```sql
SELECT
  toDate(SETTLEMENTDATE) AS day,
  round(avgIf(RRP, REGIONID = 'NSW1') - avgIf(RRP, REGIONID = 'VIC1'), 2) AS nsw_minus_vic_rrp
FROM semantic.dispatch_price
WHERE SETTLEMENTDATE >= now() - INTERVAL 7 DAY
GROUP BY day
ORDER BY day DESC;
```

### 6. Which DUIDs produced the most metered generation yesterday?

Status: works now

```sql
SELECT
  toDate(INTERVAL_DATETIME) AS day,
  DUID,
  round(sum(MWH_READING), 1) AS mwh
FROM semantic.actual_gen_duid
WHERE INTERVAL_DATETIME >= today() - INTERVAL 1 DAY
  AND INTERVAL_DATETIME < today()
GROUP BY day, DUID
ORDER BY mwh DESC
LIMIT 20;
```

### 7. Which units had the largest dispatch targets in the last day?

Status: works now

```sql
SELECT
  DUID,
  round(max(TOTALCLEARED), 2) AS peak_target_mw,
  max(SETTLEMENTDATE) AS latest_interval
FROM semantic.daily_unit_dispatch
WHERE SETTLEMENTDATE >= now() - INTERVAL 1 DAY
GROUP BY DUID
ORDER BY peak_target_mw DESC
LIMIT 20;
```

### 8. Which DUIDs had the highest SCADA output in the last day?

Status: works now

```sql
SELECT
  DUID,
  round(max(SCADAVALUE), 2) AS peak_scada_mw,
  max(SETTLEMENTDATE) AS latest_interval
FROM semantic.dispatch_unit_scada
WHERE SETTLEMENTDATE >= now() - INTERVAL 1 DAY
GROUP BY DUID
ORDER BY peak_scada_mw DESC
LIMIT 20;
```

## Higher-Value Questions

These are exactly the kinds of questions an LLM should be able to answer. Some work now from the combination of `raw_aemo_nemweb` and `raw_aemo_mmsdm`; the remaining gaps are mainly around private settlement data and promoted semantic views.

### 9. How much energy did batteries send out in Victoria over the last 7 days?

Status: works now as a public dispatch proxy, not settlement revenue

Validated result on the current warehouse:

- approximately `28.0` MWh of positive SCADA output from VIC bidirectional units over the last 7 days currently loaded

```sql
WITH toDateTime64('1900-01-01 00:00:00', 3) AS epoch,
current_du AS (
  SELECT
    DUID,
    argMax(REGIONID, tuple(coalesce(START_DATE, epoch), processed_at)) AS REGIONID
  FROM raw_aemo_mmsdm.dudetailsummary /* registration table - pending MMSDM ingest */
  WHERE END_DATE IS NULL OR END_DATE >= now()
  GROUP BY DUID
),
current_detail AS (
  SELECT
    DUID,
    argMax(DISPATCHTYPE, tuple(coalesce(EFFECTIVEDATE, epoch), processed_at)) AS DISPATCHTYPE,
    argMax(MAXSTORAGECAPACITY, tuple(coalesce(EFFECTIVEDATE, epoch), processed_at)) AS MAXSTORAGECAPACITY
  FROM raw_aemo_mmsdm.dudetail /* registration table - pending MMSDM ingest */
  GROUP BY DUID
)
SELECT
  toDate(sc.SETTLEMENTDATE) AS day,
  sc.DUID,
  round(sum(greatest(sc.SCADAVALUE, 0)) / 12, 1) AS approx_mwh_sent_out
FROM semantic.dispatch_unit_scada AS sc
INNER JOIN current_du AS ds
  ON sc.DUID = ds.DUID
INNER JOIN current_detail AS dd
  ON sc.DUID = dd.DUID
WHERE sc.SETTLEMENTDATE >= now() - INTERVAL 7 DAY
  AND ds.REGIONID = 'VIC1'
  AND dd.DISPATCHTYPE = 'BIDIRECTIONAL'
  AND dd.MAXSTORAGECAPACITY > 0
GROUP BY day, sc.DUID
ORDER BY day DESC, approx_mwh_sent_out DESC;
```

### 10. Show generation for each region broken down by fuel type.

Status: works now

Important semantic issue:

- `CO2E_ENERGY_SOURCE` and `GENSETTYPE` are usable, but they are not yet normalized into a canonical fuel taxonomy.
- This query is still useful now; a promoted `semantic.fuel_type_map` would make it cleaner later.

```sql
WITH toDateTime64('1900-01-01 00:00:00', 3) AS epoch,
unit_dim AS (
  SELECT
    ds.DUID AS DUID,
    argMax(ds.REGIONID, tuple(coalesce(ds.START_DATE, epoch), ds.processed_at)) AS REGIONID,
    argMax(
      coalesce(nullIf(g.CO2E_ENERGY_SOURCE, ''), nullIf(g.GENSETTYPE, ''), 'UNKNOWN'),
      tuple(coalesce(g.LASTCHANGED, epoch), g.processed_at)
    ) AS fuel_type
  FROM raw_aemo_mmsdm.dudetailsummary /* registration table - pending MMSDM ingest */ AS ds
  LEFT JOIN (
    SELECT
      DUID,
      argMax(GENSETID, tuple(coalesce(EFFECTIVEDATE, epoch), processed_at)) AS GENSETID
    FROM raw_aemo_mmsdm.dualloc /* registration table - pending MMSDM ingest */
    GROUP BY DUID
  ) AS da
    ON ds.DUID = da.DUID
  LEFT JOIN raw_aemo_mmsdm.genunits /* registration table - pending MMSDM ingest */ AS g
    ON da.GENSETID = g.GENSETID
  WHERE ds.END_DATE IS NULL OR ds.END_DATE >= now()
  GROUP BY ds.DUID
)
SELECT
  toDate(ng.INTERVAL_DATETIME) AS day,
  ud.REGIONID,
  ud.fuel_type,
  round(sum(ng.MWH_READING), 1) AS mwh
FROM semantic.actual_gen_duid AS ng
INNER JOIN unit_dim AS ud
  ON ng.DUID = ud.DUID
WHERE ng.INTERVAL_DATETIME >= now() - INTERVAL 7 DAY
GROUP BY day, ud.REGIONID, ud.fuel_type
ORDER BY day DESC, ud.REGIONID, mwh DESC;
```

### 11. How much money did batteries make each day last week?

Status: not answerable exactly from current raw tables

Needs:

- `SET_ENERGY_GENSET_DETAIL` or `SETGENDATA`

Reason:

- AEMO marks both candidate settlement tables as `Private`.
- Public dispatch, SCADA, and price data can support a gross merchant value proxy, but not actual settlement revenue.

### 12. Which participants earned the most generator revenue last week?

Status: not answerable yet from current raw tables

Needs:

- `SET_ENERGY_GENSET_DETAIL`
- optionally `DUDETAILSUMMARY` for region and technology slicing

SQL shape:

```sql
SELECT
  PARTICIPANTID,
  round(sum(ASOE_AMOUNT), 2) AS gross_generation_revenue
FROM raw_aemo_nemweb.<SET_ENERGY_GENSET_DETAIL_physical>
WHERE SETTLEMENTDATE >= today() - INTERVAL 7 DAY
GROUP BY PARTICIPANTID
ORDER BY gross_generation_revenue DESC
LIMIT 20;
```

### 13. Which batteries were charging the most versus discharging the most?

Status: partly answerable now from SCADA, but not yet normalized well enough for a clean generic answer

Needs:

- battery unit dimension from `raw_aemo_mmsdm`
- a promoted convention for interpreting positive versus negative output consistently across `SCADAVALUE`, `TOTALCLEARED`, and actual generation tables

Reason:

- `SCADAVALUE > 0` works as a sent-out proxy, but charging-side interpretation is still model-dependent and should be promoted into a reusable semantic recipe.

### 14. Which regions had the most carbon-intensive generation mix last week?

Status: almost answerable now

Needs:

- a decision on how to handle missing `CO2E_EMISSIONS_FACTOR`
- a promoted fuel/emissions normalization layer so the query is not littered with fallback logic

## What An LLM Still Needs To Query Reliably

### 1. A real logical table locator

Today the raw table names are physical and hash-stamped. An LLM should not have to guess them.

Needed promoted view:

```sql
semantic.table_locator(
  logical_table,
  preferred_physical_table,
  database_name,
  freshness_ts,
  row_count,
  key_columns,
  time_columns
)
```

### 2. A real unit dimension

Needed promoted view:

```sql
semantic.unit_dimension(
  duid,
  station_id,
  participant_id,
  region_id,
  dispatch_type,
  schedule_type,
  is_battery,
  is_intermittent,
  genset_id,
  fuel_type,
  co2e_emissions_factor,
  effective_from,
  effective_to
)
```

Without this, questions about batteries, fuel type, stations, and participants remain awkward.

### 3. A measure recipe catalog

Needed promoted view:

```sql
semantic.measure_recipes(
  measure_name,
  preferred_logical_table,
  expression_sql,
  grain,
  caveats
)
```

Examples:

- `spot_price -> RRP`
- `demand_forecast_error -> abs(TOTALDEMAND - DEMANDFORECAST)`
- `approx_battery_sent_out_mwh -> sum(greatest(SCADAVALUE, 0)) / 12`
- `gross_generation_revenue -> ASOE_AMOUNT`
- `net_energy_revenue -> TOTAL_AMOUNT`

### 4. Better observed schema semantics

`raw_aemo_nemweb.observed_schemas` is useful, but `logical_table` is often blank right now. That makes automatic metadata-to-raw resolution weaker than it should be.

The parser should populate these consistently for every raw physical table:

- `logical_table`
- `logical_section`
- `report_version`
- primary business time column
- natural key columns

## Immediate Recommendation

If the goal is LLM-generated SQL for market questions, the next ingestion priority should be:

1. `SET_ENERGY_GENSET_DETAIL`
2. `SETGENDATA`
3. a promoted `semantic.unit_dimension`
4. a promoted `semantic.fuel_type_map`
5. a promoted `semantic.measure_recipes`

That unlocks the high-value questions people actually ask:

- battery revenue
- generation by fuel type
- participant revenue
- emissions by region
- station and unit level rollups
