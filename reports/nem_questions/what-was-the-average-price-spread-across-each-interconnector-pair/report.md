        # What was the average price spread across each interconnector pair?

        - Status: `proxy`
        - Chart: `chart.png`
        - Raw results: `results.csv`

        ## Data

        This report is derived from the following semantic surfaces in ClickHouse: `semantic.dispatch_price`, reconciled to one row per settlement interval and region, providing 5-minute energy and FCAS regional price outcomes. `semantic.dispatch_interconnectorres`, reconciled to one row per settlement interval and interconnector, providing flow, metered flow, losses, limits, and marginal value.

        ## Note

        Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

        ## Explanation

        Top row: region_pair=TAS1-VIC1, avg_price_spread_mwh=73.18. Returned 4 rows. The mean of `avg_price_spread_mwh` across the result set is 34.490.

        ## SQL

        ```sql
        WITH

interconnector_dispatch AS (
    SELECT
        SETTLEMENTDATE,
        INTERCONNECTORID,
        avg(MWFLOW) AS MWFLOW,
        avg(METEREDMWFLOW) AS METEREDMWFLOW,
        avg(MWLOSSES) AS MWLOSSES,
        avg(EXPORTLIMIT) AS EXPORTLIMIT,
        avg(IMPORTLIMIT) AS IMPORTLIMIT,
        avg(MARGINALVALUE) AS MARGINALVALUE
    FROM semantic.dispatch_interconnectorres
    GROUP BY SETTLEMENTDATE, INTERCONNECTORID
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

prices AS (
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

SELECT

CASE
    WHEN INTERCONNECTORID IN ('NSW1-QLD1', 'N-Q-MNSP1') THEN 'NSW1-QLD1'
    WHEN INTERCONNECTORID = 'VIC1-NSW1' THEN 'VIC1-NSW1'
    WHEN INTERCONNECTORID IN ('V-SA', 'V-S-MNSP1') THEN 'VIC1-SA1'
    WHEN INTERCONNECTORID = 'T-V-MNSP1' THEN 'TAS1-VIC1'
    ELSE INTERCONNECTORID
END
 AS region_pair,
    round(avg(
        CASE
            WHEN region_pair = 'NSW1-QLD1' THEN abs(NSW1 - QLD1)
            WHEN region_pair = 'VIC1-NSW1' THEN abs(VIC1 - NSW1)
            WHEN region_pair = 'VIC1-SA1' THEN abs(VIC1 - SA1)
            WHEN region_pair = 'TAS1-VIC1' THEN abs(TAS1 - VIC1)
            ELSE NULL
        END
    ), 2) AS avg_price_spread_mwh
FROM interconnector_dispatch i
LEFT JOIN prices p ON p.SETTLEMENTDATE = i.SETTLEMENTDATE
GROUP BY region_pair
ORDER BY avg_price_spread_mwh DESC
        ```
