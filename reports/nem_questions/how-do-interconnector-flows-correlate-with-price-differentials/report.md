        # How do interconnector flows correlate with price differentials?

        - Status: `proxy`
        - Chart: `chart.png`
        - Raw results: `results.csv`

        ## Data

        This report is derived from the following semantic surfaces in ClickHouse: `semantic.dispatch_price`, reconciled to one row per settlement interval and region, providing 5-minute energy and FCAS regional price outcomes. `semantic.dispatch_interconnectorres`, reconciled to one row per settlement interval and interconnector, providing flow, metered flow, losses, limits, and marginal value.

        ## Note

        Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

        ## Explanation

        Top row: INTERCONNECTORID=V-S-MNSP1, flow_price_spread_correlation=0.06594821000271192. Returned 6 rows. The mean of `flow_price_spread_correlation` across the result set is -0.269.

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

price_pairs AS (
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
    INTERCONNECTORID,
    corr(
        MWFLOW,
        CASE
            WHEN INTERCONNECTORID IN ('NSW1-QLD1', 'N-Q-MNSP1') THEN NSW1 - QLD1
            WHEN INTERCONNECTORID = 'VIC1-NSW1' THEN VIC1 - NSW1
            WHEN INTERCONNECTORID IN ('V-SA', 'V-S-MNSP1') THEN VIC1 - SA1
            WHEN INTERCONNECTORID = 'T-V-MNSP1' THEN TAS1 - VIC1
            ELSE NULL
        END
    ) AS flow_price_spread_correlation
FROM interconnector_dispatch i
LEFT JOIN price_pairs p ON p.SETTLEMENTDATE = i.SETTLEMENTDATE
GROUP BY INTERCONNECTORID
ORDER BY flow_price_spread_correlation DESC
        ```
