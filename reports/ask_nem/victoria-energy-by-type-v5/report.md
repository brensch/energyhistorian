# How much energy was used in Victoria by type for the last 7 days, broken down by day and type

- Status: `answerable`
- Confidence: `medium`
- Raw results: `results.csv`

- Interactive chart: `chart.html`

## Answer

Victoria's generation over the last 7 days was dominated by brown coal, wind and solar. Brown coal was the largest single source each day (roughly 153,700 to 997,900 MWh/day across the week). Wind and solar were the next-largest contributors: wind ranged roughly 76,180–510,467 MWh/day (wind peaked on 2026-03-11 at ~510,467 MWh) and solar peaked on 2026-03-11 at ~87,342 MWh. Battery storage and hydro were much smaller contributors but showed spikes (battery: ~655 to 34,754 MWh/day; hydro: ~1,976 to 8,110 MWh/day). Example daily sums (summing the types reported): 2026-03-11 ≈ 1,432,648.88 MWh; 2026-03-12 ≈ 232,480.11 MWh. Overall pattern: a pronounced generation peak around 2026-03-10–11 driven by brown coal and wind, with lower total generation on 2026-03-12.

## Data

Daily energy (MWh) by fuel type for Victoria (VIC1) over the last 7 days. Energy is a proxy computed from dispatch targets: sum(greatest(TOTALCLEARED, 0) / 12) aggregated to calendar day and fuel type.

## Note

Data is generation (supply) by fuel type as a proxy for 'energy used' in VIC1. Values are computed from dispatch targets (semantic.daily_unit_dispatch) converting TOTALCLEARED (5‑minute MW) to MWh via TOTALCLEARED/12 and aggregated by calendar day and fuel type. Results cover 7 days (72 rows). These are dispatch-based operational targets (not metered settlement readings); treat as medium-confidence, not settlement-grade.

## SQL

```sql
SELECT
  toDate(d.SETTLEMENTDATE) AS day,
  u.FUEL_TYPE AS type,
  sum(greatest(d.TOTALCLEARED, 0) / 12.0) AS energy_mwh
FROM semantic.daily_unit_dispatch AS d
JOIN semantic.unit_dimension AS u ON d.DUID = u.DUID
WHERE u.REGIONID = 'VIC1'
  AND d.SETTLEMENTDATE >= now() - INTERVAL 7 DAY
GROUP BY day, type
ORDER BY day DESC, type;
```
