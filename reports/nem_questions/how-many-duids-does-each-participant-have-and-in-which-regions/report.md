        # How many DUIDs does each participant have and in which regions?

        - Status: `ready`
        - Chart: `chart.png`
        - Raw results: `results.csv`

        ## Data

        This report is derived from the following semantic surface in ClickHouse: `semantic.unit_dimension`, the current reconciled DUID dimension derived from MMSDM registration history, providing region, participant, station, fuel, registered capacity, storage flags, dispatch type, and emissions factor.

        ## Note

        Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

        ## Explanation

        Top row: participant_id=ACACIRET, REGIONID=NSW1, duid_count=1. Returned 399 rows. The mean of `duid_count` across the result set is 2.504.

        ## SQL

        ```sql
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
    REGIONID,
    countDistinct(DUID) AS duid_count
FROM unit_dim
GROUP BY participant_id, REGIONID
ORDER BY participant_id, REGIONID
        ```
