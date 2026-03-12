        # Which region had the most volatile intraday price swings?

        - Status: `ready`
        - Chart: `chart.png`
        - Raw results: `results.csv`

        ## Data

        This report is derived from the following semantic surface in ClickHouse: `semantic.dispatch_price`, reconciled to one row per settlement interval and region, providing 5-minute energy and FCAS regional price outcomes.

        ## Note

        Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

        ## Explanation

        Top row: REGIONID=SA1, avg_intraday_swing=79.61, max_intraday_swing=164.35. Returned 5 rows. The mean of `avg_intraday_swing` across the result set is 37.462.

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

daily_swings AS (
    SELECT
        toDate(SETTLEMENTDATE) AS trading_day,
        REGIONID,
        max(RRP) - min(RRP) AS intraday_swing
    FROM price_dispatch
    GROUP BY trading_day, REGIONID
)

SELECT
    REGIONID,
    round(avg(intraday_swing), 2) AS avg_intraday_swing,
    round(max(intraday_swing), 2) AS max_intraday_swing
FROM daily_swings
GROUP BY REGIONID
ORDER BY avg_intraday_swing DESC
        ```
