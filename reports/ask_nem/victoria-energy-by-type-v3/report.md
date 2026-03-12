# How much energy was used in victoria by type for the last 7 days, broken down by day and type

- Status: `answerable`
- Confidence: `medium`
- Raw results: `results.csv`

- Interactive chart: `chart.html`

## Data

Daily energy (MWh) per day and fuel type for Victoria (VIC1). Energy is a proxy computed from dispatch TOTALCLEARED summed over settlement intervals and converted to MWh by dividing by 12.

## Note

Analyst note — Method & caveats: This is a dispatch-based proxy using semantic.daily_unit_dispatch joined to unit_dimension.FUEL_TYPE. Energy = greatest(TOTALCLEARED, 0) / 12 aggregated by calendar day. Fuel/type comes from the unit_dimension table. This provides an approximate view of energy by fuel but is not metered/settlement-grade. Confidence: medium. If you need metered or settlement-quality generation, request actual/metered readings (semantic.actual_gen_duid) or settlement-interval-level data and I will rerun with that source.

## Explanation

The query returns daily energy (MWh) by fuel/type for Victoria (VIC1) for the last 7 days (today + previous 6 days). Results contain 63 rows (multiple fuel types per day). Energy was computed as max(TOTALCLEARED, 0)/12 aggregated to calendar day per unit and rolled up to unit_dimension.FUEL_TYPE. Broad patterns in the returned preview: brown coal is the largest single contributor on most days; wind and solar are also substantial (e.g. 2026-03-11 shows very large wind and solar contributions); hydro is a small contributor; battery storage is variable but can be significant on some days. Several fuel categories report zero in the preview (GENERATOR, LOAD, Natural Gas (Pipeline), UNKNOWN) — zeros reflect the dispatch proxy, not necessarily absence of generation assets.

## SQL

```sql
SELECT
  toDate(d.SETTLEMENTDATE) AS day,
  ud.FUEL_TYPE AS fuel_type,
  sum(greatest(d.TOTALCLEARED, 0) / 12.0) AS energy_mwh
FROM semantic.daily_unit_dispatch AS d
JOIN semantic.unit_dimension AS ud USING (DUID)
WHERE ud.REGIONID = 'VIC1'
  AND toDate(d.SETTLEMENTDATE) BETWEEN today() - 6 AND today()
GROUP BY day, fuel_type
ORDER BY day DESC, fuel_type;
```
