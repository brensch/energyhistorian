        # Which participants own the most registered generation capacity?

        - Status: `ready`
        - Chart: `chart.png`
        - Raw results: `results.csv`

        ## Data

        This report is derived from the following semantic surface in ClickHouse: `semantic.unit_dimension`, the current reconciled DUID dimension derived from MMSDM registration history, providing region, participant, station, fuel, registered capacity, storage flags, dispatch type, and emissions factor.

        ## Note

        Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

        ## Explanation

        Top row: participant_id=SNOWY, registered_capacity_mw=32954.0. Returned 25 rows. The mean of `registered_capacity_mw` across the result set is 3717.480.

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
    round(sum(REGISTEREDCAPACITY_MW), 2) AS registered_capacity_mw
FROM unit_dim
WHERE coalesce(DISPATCHTYPE, '') = 'GENERATOR'
GROUP BY participant_id
ORDER BY registered_capacity_mw DESC
LIMIT 25
        ```
