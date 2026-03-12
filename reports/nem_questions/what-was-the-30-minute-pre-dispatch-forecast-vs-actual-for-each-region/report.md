        # What was the 30-minute pre-dispatch forecast vs actual for each region?

        - Status: `proxy`
        - Chart: `chart.png`
        - Raw results: `results.csv`

        ## Data

        This report is derived from the following semantic surfaces in ClickHouse: `semantic.dispatch_price`, reconciled to one row per settlement interval and region, providing 5-minute energy and FCAS regional price outcomes. `semantic.predispatch_region_prices`, reconciled to one row per forecast interval and region, providing predispatch regional price forecasts.

        ## Note

        Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

        ## Explanation

        Top row: DATETIME=2026-03-12 10:30:00, REGIONID=NSW1, predispatch_rrp=-3.39, actual_rrp=nan. Returned 420 rows. The mean of `predispatch_rrp` across the result set is 34.589.

        ## SQL

        ```sql
        WITH

predispatch_price AS (
    SELECT
        DATETIME,
        REGIONID,
        avg(RRP) AS RRP
    FROM semantic.predispatch_region_prices
    GROUP BY DATETIME, REGIONID
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

SELECT
    p.DATETIME,
    p.REGIONID,
    round(p.RRP, 2) AS predispatch_rrp,
    round(a.RRP, 2) AS actual_rrp,
    round(p.RRP - a.RRP, 2) AS error_rrp
FROM predispatch_price p
LEFT JOIN price_dispatch a ON a.SETTLEMENTDATE = p.DATETIME AND a.REGIONID = p.REGIONID
ORDER BY p.DATETIME, p.REGIONID
        ```
