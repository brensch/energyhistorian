        # How many hours per week does each region spend above $100/MWh?

        - Status: `ready`
        - Chart: `chart.png`
        - Raw results: `results.csv`

        ## Data

        This report is derived from the following semantic surface in ClickHouse: `semantic.dispatch_price`, reconciled to one row per settlement interval and region, providing 5-minute energy and FCAS regional price outcomes.

        ## Note

        Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

        ## Explanation

        Top row: REGIONID=NSW1, week_start=2026-03-01, hours_above_100=0. Returned 10 rows. The mean of `hours_above_100` across the result set is 0.200.

        ## SQL

        ```sql
        WITH

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

hourly_price AS (
    SELECT
        toStartOfHour(SETTLEMENTDATE) AS hour_start,
        REGIONID,
        avg(RRP) AS hourly_avg_price
    FROM price_dispatch
    GROUP BY hour_start, REGIONID
)

SELECT
    REGIONID,
    toStartOfWeek(hour_start) AS week_start,
    countIf(hourly_avg_price > 100) AS hours_above_100
FROM hourly_price
GROUP BY REGIONID, week_start
ORDER BY week_start, REGIONID
        ```
