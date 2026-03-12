# How much energy was used in victoria by type for the last 7 days, broken down by day and type

- Status: `answerable`
- Confidence: `medium`
- Raw results: `results.csv`

- Interactive chart: `chart.html`

## Data

Daily energy (proxy) by fuel type for Victoria (VIC1) for the last 7 days. Proxy MWh is computed from dispatch targets as greatest(TOTALCLEARED, 0) / 12 aggregated to calendar day and FUEL_TYPE.

## Note

Used sources: semantic.daily_unit_dispatch and semantic.unit_dimension. Method: daily proxy MWh = greatest(TOTALCLEARED,0)/12 aggregated by day and fuel_type. Confidence: medium. Do not treat these figures as settlement-grade metered generation or actual consumer demand; if you need metered generation, query semantic.actual_gen_duid instead.

## Explanation

This result shows a dispatch-based proxy for energy by fuel type in Victoria (VIC1) for the last 7 calendar days, broken down by day and fuel_type. Proxy energy (MWh) was computed as greatest(TOTALCLEARED, 0) / 12 aggregated to calendar day and fuel_type (i.e. using semantic.daily_unit_dispatch joined to semantic.unit_dimension). The output contains 72 rows (day × fuel_type combinations) and values are in MWh; zeros indicate no positive dispatched volume for that fuel on that day. Note that this is a dispatch outcome (proxy) rather than metered generation or consumption.

## SQL

```sql
SELECT
  toDate(dud.SETTLEMENTDATE) AS day,
  ud.FUEL_TYPE AS fuel_type,
  sum(greatest(dud.TOTALCLEARED, 0) / 12.0) AS energy_mwh
FROM semantic.daily_unit_dispatch AS dud
JOIN semantic.unit_dimension AS ud
  ON dud.DUID = ud.DUID
WHERE ud.REGIONID = 'VIC1'
  AND dud.SETTLEMENTDATE >= now() - INTERVAL 7 DAY
GROUP BY day, ud.FUEL_TYPE
ORDER BY day DESC, ud.FUEL_TYPE
```
