# How much energy was used in victoria by type for the last 7 days, broken down by day and type

- Status: `answerable`
- Confidence: `medium`
- Raw results: `results.csv`

## Data

Daily totals of metered generation (MWh) in Victoria (VIC1) aggregated by day (date) and generator fuel type (FUEL_TYPE) for the last 7 days.

## Note

Confidence: medium. This result is based on metered generation by DUID and current unit fuel/type and region mapping; it is suitable for operational insight but is not settlement‑grade. If you intended end‑use demand (by customer category) that data is not available here. If you want, I can (a) return the full table, (b) compute daily totals per fuel across the 7‑day window, or (c) check for any missing fuel types or timezone alignment.

## Explanation

The query returns metered generation (MWh) inside Victoria (VIC1) aggregated by calendar day for the last 7 days and broken down by generator fuel type (columns: day, fuel_type, total_mwh). There are 24 rows (multiple fuel types per day). In the preview Wind is the dominant source each day (large MWh totals), Solar provides a smaller, variable daytime contribution (examples: ~32,035 MWh on 2026-03-06, ~25,638 MWh on 2026-03-10, ~10,917 MWh on 2026-03-11), and Hydro shows 0 MWh in the preview. Units are megawatt‑hours (MWh). Data come from semantic.actual_gen_duid joined to semantic.unit_dimension and reflect metered generation in‑region (not end‑use demand).

## SQL

```sql
SELECT
  toDate(g.INTERVAL_DATETIME) AS day,
  u.FUEL_TYPE AS fuel_type,
  SUM(g.MWH_READING) AS total_mwh
FROM semantic.actual_gen_duid AS g
JOIN semantic.unit_dimension AS u ON g.DUID = u.DUID
WHERE u.REGIONID = 'VIC1'
  AND g.INTERVAL_DATETIME >= now() - INTERVAL 7 DAY
GROUP BY day, fuel_type
ORDER BY day DESC, fuel_type
LIMIT 1000
```
