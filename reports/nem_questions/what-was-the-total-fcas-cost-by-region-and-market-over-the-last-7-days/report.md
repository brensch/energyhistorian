        # What was the total FCAS cost by region and market over the last 7 days?

        - Status: `proxy`
        - Chart: `chart.png`
        - Raw results: `results.csv`

        ## Data

        This report is derived from the following semantic surfaces in ClickHouse: `semantic.daily_region_dispatch`, reconciled to one row per settlement interval and region, providing regional demand, dispatch, interchange, available generation, excess generation, and regional FCAS enablement fields. `semantic.dispatch_price`, reconciled to one row per settlement interval and region, providing 5-minute energy and FCAS regional price outcomes.

        ## Note

        This estimates FCAS cost as enabled MW times FCAS price for each interval. It is a market-operations proxy, not a settlement-grade FCAS cost calculation. Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

        ## Explanation

        Top row: REGIONID=NSW1, market=RAISEREG, fcas_cost_proxy=613.52. Returned 40 rows. The mean of `fcas_cost_proxy` across the result set is 484.277.

        ## SQL

        ```sql
        WITH

region_dispatch AS (
    SELECT
        SETTLEMENTDATE,
        REGIONID,
        avg(RRP) AS RRP,
        avg(TOTALDEMAND) AS TOTALDEMAND,
        avg(DEMANDFORECAST) AS DEMANDFORECAST,
        avg(DISPATCHABLEGENERATION) AS DISPATCHABLEGENERATION,
        avg(NETINTERCHANGE) AS NETINTERCHANGE,
        avg(AVAILABLEGENERATION) AS AVAILABLEGENERATION,
        avg(EXCESSGENERATION) AS EXCESSGENERATION,
        avg(RAISE6SECDISPATCH) AS RAISE6SECDISPATCH,
        avg(RAISE60SECDISPATCH) AS RAISE60SECDISPATCH,
        avg(RAISE5MINDISPATCH) AS RAISE5MINDISPATCH,
        avg(RAISEREGLOCALDISPATCH) AS RAISEREGLOCALDISPATCH,
        avg(LOWER6SECDISPATCH) AS LOWER6SECDISPATCH,
        avg(LOWER60SECDISPATCH) AS LOWER60SECDISPATCH,
        avg(LOWER5MINDISPATCH) AS LOWER5MINDISPATCH,
        avg(LOWERREGLOCALDISPATCH) AS LOWERREGLOCALDISPATCH
    FROM semantic.daily_region_dispatch
    GROUP BY SETTLEMENTDATE, REGIONID
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
    d.REGIONID,
    market,
    round(sum(enabled_mw / 12.0 * price_mwh), 2) AS fcas_cost_proxy
FROM (
    SELECT SETTLEMENTDATE, REGIONID, 'RAISE6SEC' AS market, RAISE6SECDISPATCH AS enabled_mw FROM region_dispatch
    UNION ALL SELECT SETTLEMENTDATE, REGIONID, 'RAISE60SEC', RAISE60SECDISPATCH FROM region_dispatch
    UNION ALL SELECT SETTLEMENTDATE, REGIONID, 'RAISE5MIN', RAISE5MINDISPATCH FROM region_dispatch
    UNION ALL SELECT SETTLEMENTDATE, REGIONID, 'RAISEREG', RAISEREGLOCALDISPATCH FROM region_dispatch
    UNION ALL SELECT SETTLEMENTDATE, REGIONID, 'LOWER6SEC', LOWER6SECDISPATCH FROM region_dispatch
    UNION ALL SELECT SETTLEMENTDATE, REGIONID, 'LOWER60SEC', LOWER60SECDISPATCH FROM region_dispatch
    UNION ALL SELECT SETTLEMENTDATE, REGIONID, 'LOWER5MIN', LOWER5MINDISPATCH FROM region_dispatch
    UNION ALL SELECT SETTLEMENTDATE, REGIONID, 'LOWERREG', LOWERREGLOCALDISPATCH FROM region_dispatch
) d
LEFT JOIN (
    SELECT SETTLEMENTDATE, REGIONID, 'RAISE6SEC' AS market, RAISE6SECRRP AS price_mwh FROM price_dispatch
    UNION ALL SELECT SETTLEMENTDATE, REGIONID, 'RAISE60SEC', RAISE60SECRRP FROM price_dispatch
    UNION ALL SELECT SETTLEMENTDATE, REGIONID, 'RAISE5MIN', RAISE5MINRRP FROM price_dispatch
    UNION ALL SELECT SETTLEMENTDATE, REGIONID, 'RAISEREG', RAISEREGRRP FROM price_dispatch
    UNION ALL SELECT SETTLEMENTDATE, REGIONID, 'LOWER6SEC', LOWER6SECRRP FROM price_dispatch
    UNION ALL SELECT SETTLEMENTDATE, REGIONID, 'LOWER60SEC', LOWER60SECRRP FROM price_dispatch
    UNION ALL SELECT SETTLEMENTDATE, REGIONID, 'LOWER5MIN', LOWER5MINRRP FROM price_dispatch
    UNION ALL SELECT SETTLEMENTDATE, REGIONID, 'LOWERREG', LOWERREGRRP FROM price_dispatch
) p ON p.SETTLEMENTDATE = d.SETTLEMENTDATE AND p.REGIONID = d.REGIONID AND p.market = d.market
WHERE d.SETTLEMENTDATE >= now() - INTERVAL 7 DAY
GROUP BY d.REGIONID, market
ORDER BY d.REGIONID, fcas_cost_proxy DESC
        ```
