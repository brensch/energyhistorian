        # How does the time-weighted average price compare to the demand-weighted average price?

        - Status: `ready`
        - Chart: `chart.png`
        - Raw results: `results.csv`

        ## Data

        This report is derived from the following semantic surface in ClickHouse: `semantic.daily_region_dispatch`, reconciled to one row per settlement interval and region, providing regional demand, dispatch, interchange, available generation, excess generation, and regional FCAS enablement fields.

        ## Note

        Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

        ## Explanation

        SQL execution failed: 

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
    round(avg(RRP), 2) AS time_weighted_price,
    round(sum((TOTALDEMAND / 12.0) * RRP) / nullIf(sum(TOTALDEMAND / 12.0), 0), 2) AS demand_weighted_price
FROM region_dispatch
GROUP BY REGIONID
ORDER BY REGIONID
        ```
