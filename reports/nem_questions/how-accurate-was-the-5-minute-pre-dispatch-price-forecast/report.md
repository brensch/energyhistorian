        # How accurate was the 5-minute pre-dispatch price forecast?

        - Status: `proxy`
        - Chart: `chart.png`
        - Raw results: `results.csv`

        ## Data

        This report is derived from the following semantic surfaces in ClickHouse: `semantic.dispatch_price`, reconciled to one row per settlement interval and region, providing 5-minute energy and FCAS regional price outcomes. `semantic.p5min_regionsolution`, reconciled to one row per target interval, run time, and region, providing 5-minute ahead forecast and solution outputs.

        ## Note

        Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

        ## Explanation

        Top row: INTERVAL_DATETIME=2026-03-12 14:05:00, REGIONID=NSW1, forecast_rrp=-5.04, actual_rrp=nan. Returned 210 rows. The mean of `forecast_rrp` across the result set is 15.865.

        ## SQL

        ```sql
        WITH

p5_region AS (
    SELECT
        INTERVAL_DATETIME,
        RUN_DATETIME,
        REGIONID,
        avg(RRP) AS RRP,
        avg(TOTALDEMAND) AS TOTALDEMAND,
        avg(DEMANDFORECAST) AS DEMANDFORECAST
    FROM semantic.p5min_regionsolution
    GROUP BY INTERVAL_DATETIME, RUN_DATETIME, REGIONID
)
,

price_dispatch AS (
    SELECT
        SETTLEMENTDATE,
        REGIONID,
        avg(RRP) AS RRP,
        avg(RAISE6SECRRP) AS RAISE6SECRRP,
        avg(RAISE60SECRRP) AS RAISE60SECRRP,
        avg(RAISE5MINRRP) AS RAISE5MINRRP,
        avg(RAISEREGRRP) AS RAISEREGRRP,
        avg(RAISE1SECRRP) AS RAISE1SECRRP,
        avg(LOWER6SECRRP) AS LOWER6SECRRP,
        avg(LOWER60SECRRP) AS LOWER60SECRRP,
        avg(LOWER5MINRRP) AS LOWER5MINRRP,
        avg(LOWERREGRRP) AS LOWERREGRRP,
        avg(LOWER1SECRRP) AS LOWER1SECRRP
    FROM semantic.dispatch_price
    GROUP BY SETTLEMENTDATE, REGIONID
)
,

latest_forecast AS (
    SELECT
        INTERVAL_DATETIME,
        REGIONID,
        argMax(RRP, RUN_DATETIME) AS forecast_rrp
    FROM p5_region
    WHERE RUN_DATETIME <= INTERVAL_DATETIME
    GROUP BY INTERVAL_DATETIME, REGIONID
)

SELECT
    l.INTERVAL_DATETIME,
    l.REGIONID,
    round(l.forecast_rrp, 2) AS forecast_rrp,
    round(p.RRP, 2) AS actual_rrp,
    round(l.forecast_rrp - p.RRP, 2) AS error_rrp
FROM latest_forecast l
LEFT JOIN price_dispatch p ON p.SETTLEMENTDATE = l.INTERVAL_DATETIME AND p.REGIONID = l.REGIONID
ORDER BY l.INTERVAL_DATETIME, l.REGIONID
        ```
