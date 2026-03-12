        # Which region had the biggest demand forecast error each day in the last 30 days?

        - Status: `ready`
        - Chart: `chart.png`
        - Raw results: `results.csv`

        ## Data

        This report is derived from the following semantic surface in ClickHouse: `semantic.daily_region_dispatch`, reconciled to one row per settlement interval and region, providing regional demand, dispatch, interchange, available generation, excess generation, and regional FCAS enablement fields.

        ## Note

        Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

        ## Explanation

        Top row: trading_day=2026-03-02, region_with_biggest_error=NSW1, abs_error_mwh=166705.3558333333. Returned 11 rows. The mean of `abs_error_mwh` across the result set is 176822.320.

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

day_region_error AS (
    SELECT
        toDate(SETTLEMENTDATE) AS trading_day,
        REGIONID,
        sum(abs(TOTALDEMAND - DEMANDFORECAST) / 12.0) AS abs_error_mwh
    FROM region_dispatch
    WHERE SETTLEMENTDATE >= now() - INTERVAL 30 DAY
    GROUP BY trading_day, REGIONID
)

SELECT
    trading_day,
    REGIONID AS region_with_biggest_error,
    abs_error_mwh
FROM day_region_error
ORDER BY trading_day, abs_error_mwh DESC
LIMIT 1 BY trading_day
        ```
