        # How many batteries are registered in each region and what is their total capacity?

        - Status: `ready`
        - Chart: `chart.png`
        - Raw results: `results.csv`

        ## Data

        This report is derived from the following semantic surface in ClickHouse: `semantic.unit_dimension`, the current reconciled DUID dimension derived from MMSDM registration history, providing region, participant, station, fuel, registered capacity, storage flags, dispatch type, and emissions factor.

        ## Note

        Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

        ## Explanation

        Top row: REGIONID=NSW1, battery_duid_count=16, battery_capacity_mw=3013.0, battery_storage_mwh=7549.2. Returned 4 rows. The mean of `battery_duid_count` across the result set is 14.500.

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
    countDistinct(DUID) AS battery_duid_count,
    round(sum(REGISTEREDCAPACITY_MW), 2) AS battery_capacity_mw,
    round(sum(MAXSTORAGECAPACITY_MWH), 2) AS battery_storage_mwh
FROM unit_dim
WHERE IS_STORAGE = 1
GROUP BY REGIONID
ORDER BY battery_capacity_mw DESC
        ```
