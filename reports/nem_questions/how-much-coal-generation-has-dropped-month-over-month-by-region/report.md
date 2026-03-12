        # How much coal generation has dropped month-over-month by region?

        - Status: `limited`
        - Chart: `chart.png`
        - Raw results: `results.csv`

        ## Data

        This report is derived from the following semantic surfaces in ClickHouse: `semantic.actual_gen_duid`, reconciled to one row per interval and DUID, representing metered energy readings used as actual generation. `semantic.unit_dimension`, the current reconciled DUID dimension derived from MMSDM registration history, providing region, participant, station, fuel, registered capacity, storage flags, dispatch type, and emissions factor.

        ## Note

        Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

        ## Explanation

        Top row: month_start=2026-03-01, REGIONID=NSW1, coal_mwh=0.0, change_vs_prior_month_mwh=None.

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

monthly AS (
    SELECT
        toStartOfMonth(g.INTERVAL_DATETIME) AS month_start,
        u.REGIONID AS REGIONID,
        round(sum(g.MWH_READING), 2) AS coal_mwh
    FROM actual_gen g
    INNER JOIN unit_dim u ON u.DUID = g.DUID
    WHERE lower(u.FUEL_TYPE) IN ('black coal', 'brown coal')
    GROUP BY month_start, REGIONID
)

SELECT
    month_start,
    REGIONID,
    coal_mwh,
    coal_mwh - lagInFrame(coal_mwh) OVER (PARTITION BY REGIONID ORDER BY month_start) AS change_vs_prior_month_mwh
FROM monthly
ORDER BY month_start, REGIONID
        ```
