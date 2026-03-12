        # When were interconnectors at their flow limit in the last 7 days?

        - Status: `proxy`
        - Chart: `chart.png`
        - Raw results: `results.csv`

        ## Data

        This report is derived from the following semantic surface in ClickHouse: `semantic.dispatch_interconnectorres`, reconciled to one row per settlement interval and interconnector, providing flow, metered flow, losses, limits, and marginal value.

        ## Note

        Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

        ## Explanation

        Top row: SETTLEMENTDATE=2026-03-12 16:40:00, INTERCONNECTORID=N-Q-MNSP1, flow_mw=12.07, export_limit_mw=12.07. Returned 116 rows. The mean of `flow_mw` across the result set is -11.392.

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
    SETTLEMENTDATE,
    INTERCONNECTORID,
    round(MWFLOW, 2) AS flow_mw,
    round(EXPORTLIMIT, 2) AS export_limit_mw,
    round(IMPORTLIMIT, 2) AS import_limit_mw
FROM interconnector_dispatch
WHERE SETTLEMENTDATE >= now() - INTERVAL 7 DAY
  AND (
      abs(MWFLOW - EXPORTLIMIT) <= 1
      OR abs(MWFLOW + IMPORTLIMIT) <= 1
      OR abs(abs(MWFLOW) - greatest(EXPORTLIMIT, IMPORTLIMIT)) <= 1
  )
ORDER BY SETTLEMENTDATE DESC, INTERCONNECTORID
        ```
