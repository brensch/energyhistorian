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