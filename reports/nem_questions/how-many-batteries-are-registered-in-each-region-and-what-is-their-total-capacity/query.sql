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
    REGIONID,
    countDistinct(DUID) AS battery_duid_count,
    round(sum(REGISTEREDCAPACITY_MW), 2) AS battery_capacity_mw,
    round(sum(MAXSTORAGECAPACITY_MWH), 2) AS battery_storage_mwh
FROM unit_dim
WHERE IS_STORAGE = 1
GROUP BY REGIONID
ORDER BY battery_capacity_mw DESC