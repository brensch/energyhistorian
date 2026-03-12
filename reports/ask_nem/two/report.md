# give me the list of the top ten highest revenue generators over the last 7 days

- Status: `answerable`
- Confidence: `medium`
- Raw results: `results.csv`

- Interactive chart: `chart.html`

## Answer

Top 10 estimated revenue generators (last 7 days), ranked by estimated_revenue (estimated AUD, proxy MWh):
1) BW04 — Bayswater Power Station (MACQGEN): $5,852,727.84 ; 124,168.04 MWh
2) ER03 — ERARING POWER STATION (OERARING): $4,804,754.95 ; 96,975.74 MWh
3) ER04 — ERARING POWER STATION (OERARING): $4,795,095.16 ; 96,817.12 MWh
4) BW03 — Bayswater Power Station (MACQGEN): $4,653,957.35 ; 95,300.94 MWh
5) ER01 — ERARING POWER STATION (OERARING): $4,159,602.11 ; 88,611.58 MWh
6) ER02 — ERARING POWER STATION (OERARING): $4,074,638.07 ; 84,629.78 MWh
7) BW02 — Bayswater Power Station (MACQGEN): $4,021,663.69 ; 80,198.34 MWh
8) BW01 — Bayswater Power Station (MACQGEN): $3,983,418.92 ; 79,543.51 MWh
9) KPP_1 — Kogan Creek Power Station (CSENERGY): $3,793,076.86 ; 85,219.42 MWh
10) VP5 — Vales Point "B" Power Station (DELTA): $3,413,979.68 ; 66,975.64 MWh
Pattern: Bayswater (MACQGEN) and Eraring (OERARING) units dominate the top 10 by revenue.

## Data

Top 10 generating units (DUID) by estimated energy-market revenue over the last 7 days. Returns DUID, station name, participant, summed estimated revenue (price * proxy MWh) and proxy MWh (greatest(TOTALCLEARED,0)/12). Prices are regional RRP (operational 5-minute price).

## Note

Estimated-ranking methodology: revenue = sum(RRP_operational * proxy_MWh), proxy_MWh = greatest(TOTALCLEARED,0)/12 (5-min dispatch -> MWh). Prices are dispatch/operational RRPs, not settlement prices; generation is proxied from dispatch targets, not metered reads. Confidence: medium. Not settlement-grade—use metered generation and settlement prices for invoiced revenue.

## SQL

```sql
SELECT
  u.DUID,
  u.STATIONNAME,
  u.PARTICIPANTID,
  sum(greatest(d.TOTALCLEARED, 0) / 12.0 * p.RRP) AS estimated_revenue,
  sum(greatest(d.TOTALCLEARED, 0) / 12.0) AS estimated_MWh
FROM semantic.daily_unit_dispatch AS d
JOIN semantic.unit_dimension AS u ON d.DUID = u.DUID
JOIN semantic.dispatch_price AS p ON d.SETTLEMENTDATE = p.SETTLEMENTDATE AND u.REGIONID = p.REGIONID
WHERE d.SETTLEMENTDATE >= now() - INTERVAL 7 DAY
GROUP BY u.DUID, u.STATIONNAME, u.PARTICIPANTID
ORDER BY estimated_revenue DESC
LIMIT 10
```
