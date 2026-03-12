        # How correlated are prices between adjacent regions?

        - Status: `ready`
        - Chart: `chart.png`
        - Raw results: `results.csv`

        ## Data

        This report is derived from the following semantic surface in ClickHouse: `semantic.dispatch_price`, reconciled to one row per settlement interval and region, providing 5-minute energy and FCAS regional price outcomes.

        ## Note

        Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

        ## Explanation

        Top row: region_pair=VIC1-NSW1, price_correlation=0.1406179224226812. Returned 4 rows. The mean of `price_correlation` across the result set is 0.286.

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
        SETTLEMENTDATE,
        avgIf(RRP, REGIONID = 'NSW1') AS NSW1,
        avgIf(RRP, REGIONID = 'QLD1') AS QLD1,
        avgIf(RRP, REGIONID = 'VIC1') AS VIC1,
        avgIf(RRP, REGIONID = 'SA1') AS SA1,
        avgIf(RRP, REGIONID = 'TAS1') AS TAS1
    FROM price_dispatch
    GROUP BY SETTLEMENTDATE
)

SELECT 'NSW1-QLD1' AS region_pair, corr(NSW1, QLD1) AS price_correlation FROM paired
UNION ALL
SELECT 'VIC1-NSW1' AS region_pair, corr(VIC1, NSW1) AS price_correlation FROM paired
UNION ALL
SELECT 'VIC1-SA1' AS region_pair, corr(VIC1, SA1) AS price_correlation FROM paired
UNION ALL
SELECT 'TAS1-VIC1' AS region_pair, corr(TAS1, VIC1) AS price_correlation FROM paired
ORDER BY price_correlation DESC
        ```
