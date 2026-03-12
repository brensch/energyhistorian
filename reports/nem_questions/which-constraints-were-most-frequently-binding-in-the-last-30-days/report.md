        # Which constraints were most frequently binding in the last 30 days?

        - Status: `ready`
        - Chart: `chart.png`
        - Raw results: `results.csv`

        ## Data

        This report is derived from the following semantic surface in ClickHouse: `semantic.dispatch_constraint`, reconciled to one row per settlement interval and constraint, providing RHS, LHS, marginal value, and violation degree.

        ## Note

        Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

        ## Explanation

        Top row: CONSTRAINTID=C_V_LANCSF1_ZERO, binding_intervals=2880, avg_binding_shadow_price=-7308000.0. Returned 25 rows. The mean of `binding_intervals` across the result set is 249.840.

        ## SQL

        ```sql
        WITH

constraint_dispatch AS (
    SELECT
        SETTLEMENTDATE,
        CONSTRAINTID,
        avg(RHS) AS RHS,
        avg(LHS) AS LHS,
        avg(MARGINALVALUE) AS MARGINALVALUE,
        avg(VIOLATIONDEGREE) AS VIOLATIONDEGREE
    FROM semantic.dispatch_constraint
    GROUP BY SETTLEMENTDATE, CONSTRAINTID
)

SELECT
    CONSTRAINTID,
    countIf(abs(MARGINALVALUE) > 0.01) AS binding_intervals,
    round(avgIf(MARGINALVALUE, abs(MARGINALVALUE) > 0.01), 2) AS avg_binding_shadow_price
FROM constraint_dispatch
WHERE SETTLEMENTDATE >= now() - INTERVAL 30 DAY
GROUP BY CONSTRAINTID
ORDER BY binding_intervals DESC
LIMIT 25
        ```
