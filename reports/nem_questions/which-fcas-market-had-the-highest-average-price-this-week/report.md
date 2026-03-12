        # Which FCAS market had the highest average price this week?

        - Status: `ready`
        - Chart: `chart.png`
        - Raw results: `results.csv`

        ## Data

        This report is derived from the following semantic surface in ClickHouse: `semantic.dispatch_price`, reconciled to one row per settlement interval and region, providing 5-minute energy and FCAS regional price outcomes.

        ## Note

        Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

        ## Explanation

        Top row: market=RAISEREG, avg_price_mwh=4.77. Returned 8 rows. The mean of `avg_price_mwh` across the result set is 0.822.

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

SELECT market, round(avg(price_mwh), 2) AS avg_price_mwh
FROM (
    SELECT SETTLEMENTDATE, 'RAISE6SEC' AS market, RAISE6SECRRP AS price_mwh FROM price_dispatch
    UNION ALL SELECT SETTLEMENTDATE, 'RAISE60SEC', RAISE60SECRRP FROM price_dispatch
    UNION ALL SELECT SETTLEMENTDATE, 'RAISE5MIN', RAISE5MINRRP FROM price_dispatch
    UNION ALL SELECT SETTLEMENTDATE, 'RAISEREG', RAISEREGRRP FROM price_dispatch
    UNION ALL SELECT SETTLEMENTDATE, 'LOWER6SEC', LOWER6SECRRP FROM price_dispatch
    UNION ALL SELECT SETTLEMENTDATE, 'LOWER60SEC', LOWER60SECRRP FROM price_dispatch
    UNION ALL SELECT SETTLEMENTDATE, 'LOWER5MIN', LOWER5MINRRP FROM price_dispatch
    UNION ALL SELECT SETTLEMENTDATE, 'LOWERREG', LOWERREGRRP FROM price_dispatch
) f
WHERE SETTLEMENTDATE >= date_trunc('week', now())
GROUP BY market
ORDER BY avg_price_mwh DESC
        ```
