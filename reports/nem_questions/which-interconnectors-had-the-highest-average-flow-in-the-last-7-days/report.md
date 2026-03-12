        # Which interconnectors had the highest average flow in the last 7 days?

        - Status: `ready`
        - Chart: `chart.png`
        - Raw results: `results.csv`

        ## Data

        This report is derived from the following semantic surface in ClickHouse: `semantic.dispatch_interconnectorres`, reconciled to one row per settlement interval and interconnector, providing flow, metered flow, losses, limits, and marginal value.

        ## Note

        Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

        ## Explanation

        Top row: INTERCONNECTORID=VIC1-NSW1, avg_abs_flow_mw=445.77. Returned 6 rows. The mean of `avg_abs_flow_mw` across the result set is 191.958.

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

SELECT
    INTERCONNECTORID,
    round(avg(abs(MWFLOW)), 2) AS avg_abs_flow_mw
FROM interconnector_dispatch
WHERE SETTLEMENTDATE >= now() - INTERVAL 7 DAY
GROUP BY INTERCONNECTORID
ORDER BY avg_abs_flow_mw DESC
        ```
