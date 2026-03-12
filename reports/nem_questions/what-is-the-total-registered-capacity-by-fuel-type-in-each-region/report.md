        # What is the total registered capacity by fuel type in each region?

        - Status: `ready`
        - Chart: `chart.png`
        - Raw results: `results.csv`

        ## Data

        This report is derived from the following semantic surface in ClickHouse: `semantic.unit_dimension`, the current reconciled DUID dimension derived from MMSDM registration history, providing region, participant, station, fuel, registered capacity, storage flags, dispatch type, and emissions factor.

        ## Note

        Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

        ## Explanation

        Top row: REGIONID=NSW1, FUEL_TYPE=Black coal, registered_capacity_mw=12049.0. Returned 59 rows. The mean of `registered_capacity_mw` across the result set is 2208.439.

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
    REGIONID,
    FUEL_TYPE,
    round(sum(REGISTEREDCAPACITY_MW), 2) AS registered_capacity_mw
FROM unit_dim
WHERE coalesce(DISPATCHTYPE, '') = 'GENERATOR'
GROUP BY REGIONID, FUEL_TYPE
ORDER BY REGIONID, registered_capacity_mw DESC
        ```
