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