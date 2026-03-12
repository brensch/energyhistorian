        # What was the average constraint shadow price by constraint type?

        - Status: `proxy`
        - Chart: `chart.png`
        - Raw results: `results.csv`

        ## Data

        This report is derived from the following semantic surface in ClickHouse: `semantic.dispatch_constraint`, reconciled to one row per settlement interval and constraint, providing RHS, LHS, marginal value, and violation degree.

        ## Note

        Constraint type is proxied from the leading token in CONSTRAINTID, because a clean semantic constraint-type dimension is not available yet. Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

        ## Explanation

        Top row: constraint_type_proxy=NC, avg_shadow_price=3755277.32. Returned 188 rows. The mean of `avg_shadow_price` across the result set is 16813.751.

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
    splitByChar('_', ifNull(CONSTRAINTID, 'UNKNOWN'))[1] AS constraint_type_proxy,
    round(avg(MARGINALVALUE), 2) AS avg_shadow_price
FROM constraint_dispatch
GROUP BY constraint_type_proxy
ORDER BY avg_shadow_price DESC
        ```
