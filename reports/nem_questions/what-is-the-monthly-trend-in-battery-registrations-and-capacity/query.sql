
WITH battery_events AS (
    SELECT
        toStartOfMonth(EFFECTIVEDATE) AS month_start,
        DUID,
        avg(REGISTEREDCAPACITY) AS registered_capacity_mw
    FROM semantic.participant_registration_dudetail
    WHERE MAXSTORAGECAPACITY > 0 OR DISPATCHTYPE = 'BIDIRECTIONAL'
    GROUP BY month_start, DUID
)
SELECT
    month_start,
    countDistinct(DUID) AS battery_registrations,
    round(sum(registered_capacity_mw), 2) AS registered_capacity_mw
FROM battery_events
GROUP BY month_start
ORDER BY month_start