        # What was the capacity factor for each generator in the last 30 days?

        - Status: `limited`
        - Chart: `chart.png`
        - Raw results: `results.csv`

        ## Data

        This report is derived from the following semantic surfaces in ClickHouse: `semantic.actual_gen_duid`, reconciled to one row per interval and DUID, representing metered energy readings used as actual generation. `semantic.unit_dimension`, the current reconciled DUID dimension derived from MMSDM registration history, providing region, participant, station, fuel, registered capacity, storage flags, dispatch type, and emissions factor.

        ## Note

        Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

        ## Explanation

        Top row: DUID=CONDONG1, REGIONID=NSW1, FUEL_TYPE=Bagasse, capacity_factor=8.4433. Returned 35 rows. The mean of `capacity_factor` across the result set is 3.270.

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
,

generator_totals AS (
    SELECT
        g.DUID,
        any(u.REGIONID) AS REGIONID,
        any(u.FUEL_TYPE) AS FUEL_TYPE,
        any(u.REGISTEREDCAPACITY_MW) AS registered_capacity_mw,
        sum(g.MWH_READING) AS energy_mwh,
        dateDiff('hour', min(g.INTERVAL_DATETIME), max(g.INTERVAL_DATETIME)) + 1 AS span_hours
    FROM actual_gen g
    INNER JOIN unit_dim u ON u.DUID = g.DUID
    WHERE g.INTERVAL_DATETIME >= now() - INTERVAL 30 DAY
      AND coalesce(u.DISPATCHTYPE, '') = 'GENERATOR'
    GROUP BY g.DUID
)

SELECT
    DUID,
    REGIONID,
    FUEL_TYPE,
    round(energy_mwh / nullIf(registered_capacity_mw * span_hours, 0), 4) AS capacity_factor
FROM generator_totals
ORDER BY capacity_factor DESC
        ```
