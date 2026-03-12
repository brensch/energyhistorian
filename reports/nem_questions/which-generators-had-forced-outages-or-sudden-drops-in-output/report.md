        # Which generators had forced outages or sudden drops in output?

        - Status: `proxy`
        - Chart: `chart.png`
        - Raw results: `results.csv`

        ## Data

        This report is derived from the following semantic surfaces in ClickHouse: `semantic.actual_gen_duid`, reconciled to one row per interval and DUID, representing metered energy readings used as actual generation. `semantic.unit_dimension`, the current reconciled DUID dimension derived from MMSDM registration history, providing region, participant, station, fuel, registered capacity, storage flags, dispatch type, and emissions factor.

        ## Note

        Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

        ## Explanation

        Top row: DUID=WAUBRAWF, REGIONID=VIC1, FUEL_TYPE=Wind, largest_interval_drop_mwh=-174.224152. Returned 25 rows. The mean of `largest_interval_drop_mwh` across the result set is -25.307.

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
    DUID,
    any(REGIONID) AS REGIONID,
    any(FUEL_TYPE) AS FUEL_TYPE,
    min(delta_mwh) AS largest_interval_drop_mwh
FROM (
    SELECT
        g.INTERVAL_DATETIME,
        g.DUID,
        u.REGIONID,
        u.FUEL_TYPE,
        g.MWH_READING - lagInFrame(g.MWH_READING) OVER (
            PARTITION BY g.DUID ORDER BY g.INTERVAL_DATETIME
        ) AS delta_mwh
    FROM actual_gen g
    INNER JOIN unit_dim u ON u.DUID = g.DUID
)
GROUP BY DUID
ORDER BY largest_interval_drop_mwh ASC
LIMIT 25
        ```
