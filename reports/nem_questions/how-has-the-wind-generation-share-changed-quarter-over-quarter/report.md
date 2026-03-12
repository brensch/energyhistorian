        # How has the wind generation share changed quarter-over-quarter?

        - Status: `limited`
        - Chart: `chart.png`
        - Raw results: `results.csv`

        ## Data

        This report is derived from the following semantic surfaces in ClickHouse: `semantic.actual_gen_duid`, reconciled to one row per interval and DUID, representing metered energy readings used as actual generation. `semantic.unit_dimension`, the current reconciled DUID dimension derived from MMSDM registration history, providing region, participant, station, fuel, registered capacity, storage flags, dispatch type, and emissions factor.

        ## Note

        Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

        ## Explanation

        Top row: quarter_start=2026-01-01, wind_mwh=517773.77, total_mwh=1100709.69, wind_share=0.4704.

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

quarterly AS (
    SELECT
        toStartOfQuarter(g.INTERVAL_DATETIME) AS quarter_start,
        sumIf(g.MWH_READING, lower(u.FUEL_TYPE) = 'wind') AS wind_mwh,
        sum(g.MWH_READING) AS total_mwh
    FROM actual_gen g
    INNER JOIN unit_dim u ON u.DUID = g.DUID
    GROUP BY quarter_start
)

SELECT
    quarter_start,
    round(wind_mwh, 2) AS wind_mwh,
    round(total_mwh, 2) AS total_mwh,
    round(wind_mwh / nullIf(total_mwh, 0), 4) AS wind_share
FROM quarterly
ORDER BY quarter_start
        ```
