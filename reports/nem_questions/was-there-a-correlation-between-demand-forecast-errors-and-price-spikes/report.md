        # Was there a correlation between demand forecast errors and price spikes?

        - Status: `ready`
        - Chart: `chart.png`
        - Raw results: `results.csv`

        ## Data

        This report is derived from the following semantic surfaces in ClickHouse: `semantic.daily_region_dispatch`, reconciled to one row per settlement interval and region, providing regional demand, dispatch, interchange, available generation, excess generation, and regional FCAS enablement fields. `semantic.dispatch_price`, reconciled to one row per settlement interval and region, providing 5-minute energy and FCAS regional price outcomes.

        ## Note

        Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

        ## Explanation

        Top row: REGIONID=TAS1, error_price_correlation=0.695164449524824. Returned 5 rows. The mean of `error_price_correlation` across the result set is 0.519.

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
,

joined AS (
    SELECT
        d.SETTLEMENTDATE,
        d.REGIONID,
        abs(d.TOTALDEMAND - d.DEMANDFORECAST) AS abs_forecast_error_mw,
        p.RRP AS price_rrp
    FROM region_dispatch d
    LEFT JOIN price_dispatch p ON p.SETTLEMENTDATE = d.SETTLEMENTDATE AND p.REGIONID = d.REGIONID
)

SELECT
    REGIONID,
    corr(abs_forecast_error_mw, price_rrp) AS error_price_correlation
FROM joined
GROUP BY REGIONID
ORDER BY error_price_correlation DESC
        ```
