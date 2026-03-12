# How much energy was used in Victoria by type for the last 7 days, broken down by day and type

- Status: `answerable`
- Confidence: `medium`
- Raw results: `results.csv`

- Interactive chart: `chart.html`

## Answer

Query returned daily proxy energy (MWh) by fuel type for Victoria (REGIONID = 'VIC1') for the last 7 days — 63 rows (9 fuel types × 7 days). Values are proxy MWh = greatest(TOTALCLEARED,0)/12. Example rows from the result preview (day → {type: energy_mwh, MWh}):

2026-03-12 → {Battery Storage: 654.543440833333 MWh, Brown coal: 153669.2575458336 MWh, GENERATOR: 0.0 MWh, Hydro: 1976.3333333333333 MWh, LOAD: 0.0 MWh, Natural Gas (Pipeline): 0.0 MWh, Solar: 0.0 MWh, UNKNOWN: 0.0 MWh, Wind: 76179.975565833 MWh}

2026-03-11 → {Battery Storage: 2649.8250625 MWh, Brown coal: 824080.088136667 MWh, GENERATOR: 0.0 MWh, Hydro: 8110.193333333333 MWh, LOAD: 0.0 MWh, Natural Gas (Pipeline): 0.0 MWh, Solar: 87342.2438625002 MWh, UNKNOWN: 0.0 MWh, Wind: 510466.53023500094 MWh}

2026-03-10 → {Battery Storage: 34754.43055166669 MWh, Brown coal: 997876.7909708332 MWh}

(The full output contains all 63 day×type rows in the same format. If you want the complete table as CSV or JSON, I can provide it.)

## Data

Sum of proxy energy (MWh) per day and fuel type for Victoria (VIC1) for the last 7 days (today and previous 6 days). Proxy MWh computed as greatest(TOTALCLEARED, 0) / 12 aggregated by calendar day and FUEL_TYPE from unit_dimension.

## Note

Method & caveats: Results are from semantic.daily_unit_dispatch joined to semantic.unit_dimension; energy is proxied as greatest(TOTALCLEARED,0)/12 aggregated by calendar day and FUEL_TYPE for REGIONID='VIC1'. Confidence: medium. This is a dispatch-based proxy for energy-by-fuel, not settlement-grade or metered actual generation—request semantic.actual_gen_duid for meter readings or settlement-grade values.

## SQL

```sql
SELECT
  toDate(d.SETTLEMENTDATE) AS day,
  u.FUEL_TYPE AS type,
  sum(greatest(d.TOTALCLEARED, 0) / 12.0) AS energy_mwh
FROM semantic.daily_unit_dispatch AS d
JOIN semantic.unit_dimension AS u ON d.DUID = u.DUID
WHERE u.REGIONID = 'VIC1'
  AND toDate(d.SETTLEMENTDATE) >= today() - 6
GROUP BY day, type
ORDER BY day DESC, type ASC
```
