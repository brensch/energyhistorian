        # What was the total interconnector loss by flow direction this week?

        - Status: `proxy`
        - Chart: `chart.png`
        - Raw results: `results.csv`

        ## Data

        This report is derived from the following semantic surface in ClickHouse: `semantic.dispatch_interconnectorres`, reconciled to one row per settlement interval and interconnector, providing flow, metered flow, losses, limits, and marginal value.

        ## Note

        Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

        ## Explanation

        Top row: INTERCONNECTORID=N-Q-MNSP1, flow_direction=Negative flow, total_losses_mwh=4.83. Returned 9 rows. The mean of `total_losses_mwh` across the result set is 4.339.

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
    if(MWFLOW >= 0, 'Positive flow', 'Negative flow') AS flow_direction,
    round(sum(MWLOSSES / 12.0), 2) AS total_losses_mwh
FROM interconnector_dispatch
WHERE SETTLEMENTDATE >= date_trunc('week', now())
GROUP BY INTERCONNECTORID, flow_direction
ORDER BY INTERCONNECTORID, flow_direction
        ```
