        # How wide was the NSW-VIC price spread each day over the last week?

        - Status: `ready`
        - Chart: `chart.png`
        - Raw results: `results.csv`

        ## Data

        This report is derived from the following semantic surface in ClickHouse: `semantic.dispatch_price`, reconciled to one row per settlement interval and region, providing 5-minute energy and FCAS regional price outcomes.

        ## Note

        Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

        ## Explanation

        Top row: trading_day=2026-03-05, nsw_vic_spread_mwh=63.51. Returned 3 rows. The mean of `nsw_vic_spread_mwh` across the result set is 38.447.

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

paired AS (
    SELECT
        toDate(SETTLEMENTDATE) AS trading_day,
        avgIf(RRP, REGIONID = 'NSW1') AS nsw_rrp,
        avgIf(RRP, REGIONID = 'VIC1') AS vic_rrp
    FROM price_dispatch
    WHERE SETTLEMENTDATE >= now() - INTERVAL 7 DAY
    GROUP BY trading_day
)

SELECT
    trading_day,
    round(nsw_rrp - vic_rrp, 2) AS nsw_vic_spread_mwh
FROM paired
ORDER BY trading_day
        ```
