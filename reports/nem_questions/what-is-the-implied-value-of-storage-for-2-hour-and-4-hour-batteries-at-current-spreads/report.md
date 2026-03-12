        # What is the implied value of storage for 2-hour and 4-hour batteries at current spreads?

        - Status: `proxy`
        - Chart: `chart.png`
        - Raw results: `results.csv`

        ## Data

        This report is derived from the following semantic surface in ClickHouse: `semantic.dispatch_price`, reconciled to one row per settlement interval and region, providing 5-minute energy and FCAS regional price outcomes.

        ## Note

        Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

        ## Explanation

        Top row: trading_day=2026-03-05, REGIONID=NSW1, implied_value_2h_mwday=7.68, implied_value_4h_mwday=15.36. Returned 15 rows. The mean of `implied_value_2h_mwday` across the result set is 74.924.

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

daily_spreads AS (
    SELECT
        toDate(SETTLEMENTDATE) AS trading_day,
        REGIONID,
        max(RRP) - min(RRP) AS intraday_spread
    FROM price_dispatch
    GROUP BY trading_day, REGIONID
)

SELECT
    trading_day,
    REGIONID,
    round(intraday_spread * 2, 2) AS implied_value_2h_mwday,
    round(intraday_spread * 4, 2) AS implied_value_4h_mwday
FROM daily_spreads
ORDER BY trading_day, REGIONID
        ```
