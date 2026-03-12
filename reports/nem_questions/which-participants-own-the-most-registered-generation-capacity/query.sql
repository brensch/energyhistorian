WITH

unit_dim AS (
    SELECT
        DUID,
        REGIONID,
        EFFECTIVE_PARTICIPANTID,
        PARTICIPANTID,
        STATIONID,
        STATIONNAME,
        FUEL_TYPE,
        ENERGY_SOURCE,
        REGISTEREDCAPACITY_MW,
        MAXCAPACITY_MW,
        MAXSTORAGECAPACITY_MWH,
        CO2E_EMISSIONS_FACTOR,
        IS_STORAGE,
        IS_BIDIRECTIONAL,
        DISPATCHTYPE,
        SCHEDULE_TYPE
    FROM semantic.unit_dimension
)

SELECT
    coalesce(EFFECTIVE_PARTICIPANTID, PARTICIPANTID, 'UNKNOWN') AS participant_id,
    round(sum(REGISTEREDCAPACITY_MW), 2) AS registered_capacity_mw
FROM unit_dim
WHERE coalesce(DISPATCHTYPE, '') = 'GENERATOR'
GROUP BY participant_id
ORDER BY registered_capacity_mw DESC
LIMIT 25