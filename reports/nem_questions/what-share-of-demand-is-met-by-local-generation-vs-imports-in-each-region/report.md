        # What share of demand is met by local generation vs imports in each region?

        - Status: `proxy`
        - Chart: `chart.png`
        - Raw results: `results.csv`

        ## Data

        This report is derived from the following semantic surface in ClickHouse: `semantic.daily_region_dispatch`, reconciled to one row per settlement interval and region, providing regional demand, dispatch, interchange, available generation, excess generation, and regional FCAS enablement fields.

        ## Note

        This approximates local generation share from total demand and absolute net interchange. It is direction-agnostic and should be treated as a rough operational proxy. Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

        ## Explanation

        Top row: REGIONID=TAS1, local_generation_proxy_mw=864.19, imports_proxy_mw=64.19, local_share_proxy=0.9309. Returned 5 rows. The mean of `local_generation_proxy_mw` across the result set is 3787.114.

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

SELECT
    REGIONID,
    round(avg(TOTALDEMAND - abs(NETINTERCHANGE)), 2) AS local_generation_proxy_mw,
    round(avg(abs(NETINTERCHANGE)), 2) AS imports_proxy_mw,
    round(avg(TOTALDEMAND - abs(NETINTERCHANGE)) / nullIf(avg(TOTALDEMAND), 0), 4) AS local_share_proxy
FROM region_dispatch
GROUP BY REGIONID
ORDER BY local_share_proxy DESC
        ```
