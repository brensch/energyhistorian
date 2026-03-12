        # What is the trend in average daily price by region over the last 12 months?

        - Status: `limited`
        - Chart: `chart.png`
        - Raw results: `results.csv`

        ## Data

        This report is derived from the following semantic surface in ClickHouse: `semantic.dispatch_price`, reconciled to one row per settlement interval and region, providing 5-minute energy and FCAS regional price outcomes.

        ## Note

        Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

        ## Explanation

        Top row: trading_day=2026-03-05, REGIONID=NSW1, avg_daily_price=58.98. Returned 15 rows. The mean of `avg_daily_price` across the result set is 21.895.

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

SELECT
    toDate(SETTLEMENTDATE) AS trading_day,
    REGIONID,
    round(avg(RRP), 2) AS avg_daily_price
FROM price_dispatch
WHERE SETTLEMENTDATE >= now() - INTERVAL 12 MONTH
GROUP BY trading_day, REGIONID
ORDER BY trading_day, REGIONID
        ```
