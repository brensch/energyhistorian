        # What was the solar generation curve yesterday by region?

        - Status: `ready`
        - Chart: `chart.png`
        - Raw results: `results.csv`

        ## Data

        This report is derived from the following semantic surfaces in ClickHouse: `semantic.actual_gen_duid`, reconciled to one row per interval and DUID, representing metered energy readings used as actual generation. `semantic.unit_dimension`, the current reconciled DUID dimension derived from MMSDM registration history, providing region, participant, station, fuel, registered capacity, storage flags, dispatch type, and emissions factor.

        ## Note

        Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

        ## Explanation

        Top row: hour_start=2026-03-11 00:00:00, REGIONID=NSW1, solar_mwh=0.43. Returned 72 rows. The mean of `solar_mwh` across the result set is 129.513.

        ## SQL

        ```sql
        WITH

actual_gen AS (
    SELECT
        INTERVAL_DATETIME,
        DUID,
        avg(MWH_READING) AS MWH_READING
    FROM semantic.actual_gen_duid
    GROUP BY INTERVAL_DATETIME, DUID
)
,

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
    toStartOfHour(g.INTERVAL_DATETIME) AS hour_start,
    u.REGIONID,
    round(sum(g.MWH_READING), 2) AS solar_mwh
FROM actual_gen g
INNER JOIN unit_dim u ON u.DUID = g.DUID
WHERE toDate(g.INTERVAL_DATETIME) = yesterday()
  AND lower(u.FUEL_TYPE) = 'solar'
GROUP BY hour_start, u.REGIONID
ORDER BY hour_start, u.REGIONID
        ```
