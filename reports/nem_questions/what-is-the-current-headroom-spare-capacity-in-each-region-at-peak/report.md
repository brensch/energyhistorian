        # What is the current headroom (spare capacity) in each region at peak?

        - Status: `proxy`
        - Chart: `chart.png`
        - Raw results: `results.csv`

        ## Data

        This report is derived from the following semantic surface in ClickHouse: `semantic.daily_region_dispatch`, reconciled to one row per settlement interval and region, providing regional demand, dispatch, interchange, available generation, excess generation, and regional FCAS enablement fields.

        ## Note

        Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

        ## Explanation

        Top row: REGIONID=TAS1, SETTLEMENTDATE=2026-03-10 06:35:00, spare_capacity_mw=522.01. Returned 5 rows. The mean of `spare_capacity_mw` across the result set is 2119.310.

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

peak_intervals AS (
    SELECT
        REGIONID,
        argMax(SETTLEMENTDATE, TOTALDEMAND) AS peak_interval
    FROM region_dispatch
    GROUP BY REGIONID
)

SELECT
    p.REGIONID,
    r.SETTLEMENTDATE,
    round(r.AVAILABLEGENERATION - r.TOTALDEMAND, 2) AS spare_capacity_mw
FROM peak_intervals p
LEFT JOIN region_dispatch r
    ON r.REGIONID = p.REGIONID
   AND r.SETTLEMENTDATE = p.peak_interval
ORDER BY spare_capacity_mw ASC
        ```
