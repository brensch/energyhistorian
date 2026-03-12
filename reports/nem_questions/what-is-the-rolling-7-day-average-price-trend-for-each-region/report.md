        # What is the rolling 7-day average price trend for each region?

        - Status: `ready`
        - Chart: `chart.png`
        - Raw results: `results.csv`

        ## Data

        This report is derived from the following semantic surface in ClickHouse: `semantic.dispatch_price`, reconciled to one row per settlement interval and region, providing 5-minute energy and FCAS regional price outcomes.

        ## Note

        Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

        ## Explanation

        Top row: trading_day=2026-03-05, REGIONID=NSW1, avg_daily_price=58.98, rolling_7d_avg_price=58.98. Returned 15 rows. The mean of `avg_daily_price` across the result set is 21.895.

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

daily_price AS (
    SELECT
        toDate(SETTLEMENTDATE) AS trading_day,
        REGIONID,
        avg(RRP) AS avg_daily_price
    FROM price_dispatch
    GROUP BY trading_day, REGIONID
)

SELECT
    trading_day,
    REGIONID,
    round(avg_daily_price, 2) AS avg_daily_price,
    round(avg(avg_daily_price) OVER (
        PARTITION BY REGIONID ORDER BY trading_day
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2) AS rolling_7d_avg_price
FROM daily_price
ORDER BY trading_day, REGIONID
        ```
