        # How did the dispatch engine's solution quality change over the last 30 days?

        - Status: `proxy`
        - Chart: `chart.png`
        - Raw results: `results.csv`

        ## Data

        This report is derived from the following semantic surface in ClickHouse: `semantic.daily_region_dispatch`, reconciled to one row per settlement interval and region, providing regional demand, dispatch, interchange, available generation, excess generation, and regional FCAS enablement fields.

        ## Note

        This uses observable dispatch outcomes as a proxy for solution quality because a dedicated solver-quality metric is not exposed in the semantic layer. Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

        ## Explanation

        Top row: trading_day=2026-03-02, avg_abs_price=68.13, avg_demand_error_mw=4569.49, avg_excess_generation_mw=0.0. Returned 11 rows. The mean of `avg_abs_price` across the result set is 66.216.

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
    toDate(SETTLEMENTDATE) AS trading_day,
    round(avg(abs(RRP)), 2) AS avg_abs_price,
    round(avg(abs(TOTALDEMAND - DEMANDFORECAST)), 2) AS avg_demand_error_mw,
    round(avg(abs(EXCESSGENERATION)), 2) AS avg_excess_generation_mw
FROM region_dispatch
WHERE SETTLEMENTDATE >= now() - INTERVAL 30 DAY
GROUP BY trading_day
ORDER BY trading_day
        ```
