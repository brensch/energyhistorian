        # How many MW of gas generation capacity is registered but rarely dispatched?

        - Status: `proxy`
        - Chart: `chart.png`
        - Raw results: `results.csv`

        ## Data

        This report is derived from the following semantic surfaces in ClickHouse: `semantic.actual_gen_duid`, reconciled to one row per interval and DUID, representing metered energy readings used as actual generation. `semantic.unit_dimension`, the current reconciled DUID dimension derived from MMSDM registration history, providing region, participant, station, fuel, registered capacity, storage flags, dispatch type, and emissions factor.

        ## Note

        Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

        ## Explanation

        Top row: REGIONID=NSW1, rarely_dispatched_capacity_mw=50.0. Returned 5 rows. The mean of `rarely_dispatched_capacity_mw` across the result set is 50.000.

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

gas_units AS (
    SELECT
        u.DUID,
        u.REGIONID,
        u.REGISTEREDCAPACITY_MW,
        sum(g.MWH_READING) AS energy_mwh,
        dateDiff('hour', min(g.INTERVAL_DATETIME), max(g.INTERVAL_DATETIME)) + 1 AS span_hours
    FROM unit_dim u
    LEFT JOIN actual_gen g ON g.DUID = u.DUID
    WHERE lower(u.FUEL_TYPE) = 'natural gas (pipeline)'
    GROUP BY u.DUID, u.REGIONID, u.REGISTEREDCAPACITY_MW
)

SELECT
    REGIONID,
    round(sumIf(REGISTEREDCAPACITY_MW, energy_mwh / nullIf(REGISTEREDCAPACITY_MW * span_hours, 0) < 0.1), 2) AS rarely_dispatched_capacity_mw
FROM gas_units
GROUP BY REGIONID
ORDER BY rarely_dispatched_capacity_mw DESC
        ```
