# How much energy was used in victoria by type for the last 7 days, broken down by day and type

- Status: `answerable`
- Confidence: `medium`
- Raw results: `results.csv`

## Data

Daily energy (MWh) by fuel/source type for Victoria (VIC1) over the last 7 days. Columns: day (date), type (FUEL_TYPE from unit_dimension), energy_mwh (sum of MWH_READING).

## Note

Analyst note — confidence: medium. Caveats: these are metered generation sums, not settlement-grade or direct demand figures. They may omit behind-the-meter or non-metered resources, exclude netting for interconnector flows and storage charging/discharging, and depend on correct unit ↔ fuel mappings. Large wind totals reflect aggregation across many generators and should be validated if surprising. If you need actual consumer demand, dispatch/settlement-quality data, or timezone/DST-verified sums, request the appropriate demand or market operator datasets for verification.

## Explanation

The result is a 7‑day daily breakdown (2026-03-06 through 2026-03-12) of summed metered generation in Victoria (VIC1), grouped by generator fuel/source type (FUEL_TYPE). Each row gives day, fuel type, and energy_mwh (the sum of MWH_READING from semantic.actual_gen_duid joined to unit_dimension). The preview contains mainly Wind and Solar entries (Hydro shows zeros). Row_count = 24 (multiple fuel types per day where available). Units = MWh. Note that these values are aggregated in-region metered generation and are presented as a proxy for “energy used” by type, not as direct consumer demand or net system consumption.

## SQL

```sql
SELECT
  toDate(a.INTERVAL_DATETIME) AS day,
  u.FUEL_TYPE AS type,
  sum(a.MWH_READING) AS energy_mwh
FROM semantic.actual_gen_duid AS a
JOIN semantic.unit_dimension AS u ON a.DUID = u.DUID
WHERE u.REGIONID = 'VIC1'
  AND a.INTERVAL_DATETIME >= now() - INTERVAL 7 DAY
GROUP BY day, type
ORDER BY day DESC, type
LIMIT 1000
```
