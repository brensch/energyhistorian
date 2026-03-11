# Australian Energy Market Questions

This note is grounded in the current ClickHouse state on March 11, 2026.

Current raw tables that are already useful:

- `raw_aemo_nemweb.public_prices__dregion____v3__h_4729f9170566`
- `raw_aemo_nemweb.next_day_actual_gen__meter_data__gen_duid__v1__h_68a443cd5443`
- `raw_aemo_nemweb.next_day_dispatch__dispatch__unit_solution__v6__h_207053e2f283`
- `raw_aemo_nemweb.dispatch_scada__dispatch__unit_scada__v1__h_5b0582bb8835`

Metadata tables that describe future queryable objects:

- `raw_aemo_metadata_html.table_explanations`
- `raw_aemo_metadata_html.column_explanations`
- `raw_aemo_metadata_dvd.tables`
- `raw_aemo_metadata_dvd.columns`

Important constraint:

- Questions about batteries, fuel type, station ownership, and settlement revenue need registration and settlement tables such as `DUDETAIL`, `DUDETAILSUMMARY`, `DUALLOC`, `GENUNITS`, `SET_ENERGY_GENSET_DETAIL`, or `SETGENDATA` to be ingested into `raw_aemo_nemweb`.
- Today, those tables are present in metadata but not yet present in the raw NEMweb warehouse.

## Questions That Work Now

### 1. What was the average spot price in each region over the last 7 days?

Status: works now

```sql
SELECT
  toDate(SETTLEMENTDATE) AS day,
  REGIONID,
  round(avg(RRP), 2) AS avg_rrp
FROM raw_aemo_nemweb.public_prices__dregion____v3__h_4729f9170566
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
  FROM raw_aemo_nemweb.public_prices__dregion____v3__h_4729f9170566
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
FROM raw_aemo_nemweb.public_prices__dregion____v3__h_4729f9170566
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
FROM raw_aemo_nemweb.public_prices__dregion____v3__h_4729f9170566
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
FROM raw_aemo_nemweb.public_prices__dregion____v3__h_4729f9170566
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
FROM raw_aemo_nemweb.next_day_actual_gen__meter_data__gen_duid__v1__h_68a443cd5443
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
FROM raw_aemo_nemweb.next_day_dispatch__dispatch__unit_solution__v6__h_207053e2f283
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
FROM raw_aemo_nemweb.dispatch_scada__dispatch__unit_scada__v1__h_5b0582bb8835
WHERE SETTLEMENTDATE >= now() - INTERVAL 1 DAY
GROUP BY DUID
ORDER BY peak_scada_mw DESC
LIMIT 20;
```

## Questions That Need More Raw Tables

These are exactly the kinds of questions an LLM should be able to answer, but they need a better unit dimension and settlement layer in raw storage.

### 9. How much did batteries make in Victoria last week?

Status: not answerable yet from current raw tables

Needs:

- `DUDETAILSUMMARY` for `REGIONID`, `DISPATCHTYPE`
- `DUDETAIL` for `MAXSTORAGECAPACITY`
- `SET_ENERGY_GENSET_DETAIL` for settlement energy dollar amounts

Ambiguity to decide once:

- `TOTAL_AMOUNT` is net energy settlement amount
- `ASOE_AMOUNT` is sent-out generation revenue
- `ACE_AMOUNT` is consumed energy cost

SQL shape once those tables are ingested:

```sql
WITH vic_batteries AS (
  SELECT DISTINCT s.DUID
  FROM raw_aemo_nemweb.<DUDETAILSUMMARY_physical> AS s
  INNER JOIN raw_aemo_nemweb.<DUDETAIL_physical> AS d
    ON s.DUID = d.DUID
  WHERE s.REGIONID = 'VIC1'
    AND s.DISPATCHTYPE = 'BIDIRECTIONAL'
    AND d.MAXSTORAGECAPACITY > 0
),
daily_revenue AS (
  SELECT
    toDate(SETTLEMENTDATE) AS day,
    DUID,
    sum(TOTAL_AMOUNT) AS net_energy_revenue,
    sum(ASOE_AMOUNT) AS gross_generation_revenue,
    sum(ACE_AMOUNT) AS charging_cost
  FROM raw_aemo_nemweb.<SET_ENERGY_GENSET_DETAIL_physical>
  WHERE SETTLEMENTDATE >= today() - INTERVAL 7 DAY
  GROUP BY day, DUID
)
SELECT
  day,
  DUID,
  round(net_energy_revenue, 2) AS net_energy_revenue,
  round(gross_generation_revenue, 2) AS gross_generation_revenue,
  round(charging_cost, 2) AS charging_cost
FROM daily_revenue
WHERE DUID IN (SELECT DUID FROM vic_batteries)
ORDER BY day DESC, net_energy_revenue DESC;
```

### 10. Show generation for each region broken down by fuel type.

Status: not answerable yet from current raw tables

Needs:

- `next_day_actual_gen` or `DISPATCH_UNIT_SCADA` for generation
- `DUDETAILSUMMARY` for `REGIONID`
- `DUALLOC` to map `DUID -> GENSETID`
- `GENUNITS` for `CO2E_ENERGY_SOURCE` or `GENSETTYPE`

Important semantic issue:

- `CO2E_ENERGY_SOURCE` and `GENSETTYPE` are usable inputs but are not a clean canonical fuel taxonomy.
- This needs a promoted mapping like `semantic.fuel_type_map`.

SQL shape once those tables are ingested:

```sql
WITH unit_dim AS (
  SELECT
    ds.DUID,
    ds.REGIONID,
    coalesce(g.CO2E_ENERGY_SOURCE, g.GENSETTYPE, 'UNKNOWN') AS fuel_type
  FROM raw_aemo_nemweb.<DUDETAILSUMMARY_physical> AS ds
  LEFT JOIN raw_aemo_nemweb.<DUALLOC_physical> AS a
    ON ds.DUID = a.DUID
  LEFT JOIN raw_aemo_nemweb.<GENUNITS_physical> AS g
    ON a.GENSETID = g.GENSETID
),
actual_gen AS (
  SELECT
    toDate(INTERVAL_DATETIME) AS day,
    DUID,
    sum(MWH_READING) AS mwh
  FROM raw_aemo_nemweb.<NEXT_DAY_ACTUAL_GEN_physical>
  WHERE INTERVAL_DATETIME >= today() - INTERVAL 7 DAY
  GROUP BY day, DUID
)
SELECT
  g.day,
  u.REGIONID,
  u.fuel_type,
  round(sum(g.mwh), 1) AS mwh
FROM actual_gen g
INNER JOIN unit_dim u
  ON g.DUID = u.DUID
GROUP BY g.day, u.REGIONID, u.fuel_type
ORDER BY g.day DESC, u.REGIONID, mwh DESC;
```

### 11. How much money did batteries make each day last week?

Status: not answerable yet from current raw tables

Needs:

- same tables as Question 9

This is the same shape as Question 9 without the Victoria filter.

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

Status: not answerable cleanly yet

Needs:

- battery unit dimension from `DUDETAILSUMMARY` and `DUDETAIL`
- either settlement energy detail or a clean dispatch-side battery interpretation

Reason:

- dispatch-side `TOTALCLEARED` alone is not enough unless bidirectional semantics are normalized consistently.

### 14. Which regions had the most carbon-intensive generation mix last week?

Status: not answerable yet

Needs:

- actual generation
- `DUALLOC`
- `GENUNITS.CO2E_EMISSIONS_FACTOR`
- region dimension from `DUDETAILSUMMARY`

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

1. `DUDETAILSUMMARY`
2. `DUDETAIL`
3. `DUALLOC`
4. `GENUNITS`
5. `SET_ENERGY_GENSET_DETAIL`
6. `SETGENDATA`

That unlocks the high-value questions people actually ask:

- battery revenue
- generation by fuel type
- participant revenue
- emissions by region
- station and unit level rollups
