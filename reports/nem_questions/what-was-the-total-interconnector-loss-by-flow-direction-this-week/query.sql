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