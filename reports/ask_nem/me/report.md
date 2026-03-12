        # How much energy was used in victoria by type for the last 7 days, broken down by day and type

        - Status: `answerable`
        - Confidence: `medium`
        - Raw results: `results.csv`

        ## Data

        Daily total metered generation (MWh) by fuel type for the Victoria region for the last 7 calendar days (grouped by day and fuel type).

        ## Note

        Confidence: medium. Do not treat this as settlement-grade data. Common reasons for no-results: (1) data for the requested days has not been ingested yet or there is an ingestion lag/timezone mismatch; (2) REGIONID value differs (e.g., alternative region code or NULL); (3) the join removed rows (unit_dimension entries missing or mismatched keys); or (4) you intended consumer demand rather than generation — in that case use semantic.daily_region_dispatch.TOTALDEMAND. Suggested next steps: confirm the exact date range/timezone, verify REGIONID values in unit_dimension, run the query without the join to check raw actual_gen_duid records, expand the timeframe to 30 days to check data presence, or ask me to prepare an alternative query using TOTALDEMAND if you mean consumption.

        ## Explanation

        The query returned no rows. It was intended to sum actual_gen_duid.MWH_READING by day and unit_dimension.FUEL_TYPE for units where unit_dimension.REGIONID = 'VIC' over the last 7 calendar days. An empty result means no generation readings matched those filters in the requested period (no rows to aggregate), so we cannot report daily-by-fuel totals for Victoria for that window.

        ## SQL

        ```sql
        SELECT
  toDate(a.INTERVAL_DATETIME) AS day,
  u.FUEL_TYPE AS fuel_type,
  sum(a.MWH_READING) AS energy_mwh
FROM semantic.actual_gen_duid AS a
JOIN semantic.unit_dimension AS u
  ON a.DUID = u.DUID
WHERE u.REGIONID = 'VIC'
  AND toDate(a.INTERVAL_DATETIME) >= today() - 6
GROUP BY day, fuel_type
ORDER BY day DESC, fuel_type
        ```
