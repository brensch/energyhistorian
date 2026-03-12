        # What is the current installed solar capacity vs 12 months ago by region?

        - Status: `limited`
        - Chart: `chart.png`
        - Raw results: `results.csv`

        ## Data

        This report is derived from the following semantic surface in ClickHouse: `semantic.unit_dimension`, the current reconciled DUID dimension derived from MMSDM registration history, providing region, participant, station, fuel, registered capacity, storage flags, dispatch type, and emissions factor.

        ## Note

        The semantic layer currently exposes current unit state cleanly; the 12-month comparison is omitted until a historical registration snapshot mart is built. Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

        ## Explanation

        Top row: REGIONID=NSW1, current_solar_capacity_mw=5976.0. Returned 6 rows. The mean of `current_solar_capacity_mw` across the result set is 3287.000.

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
    round(sumIf(REGISTEREDCAPACITY_MW, lower(FUEL_TYPE) = 'solar'), 2) AS current_solar_capacity_mw
FROM unit_dim
GROUP BY REGIONID
ORDER BY current_solar_capacity_mw DESC
        ```
