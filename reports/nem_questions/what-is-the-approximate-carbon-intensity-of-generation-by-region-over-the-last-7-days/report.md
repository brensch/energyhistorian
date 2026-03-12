        # What is the approximate carbon intensity of generation by region over the last 7 days?

        - Status: `proxy`
        - Chart: `chart.png`
        - Raw results: `results.csv`

        ## Data

        This report is derived from the following semantic surfaces in ClickHouse: `semantic.actual_gen_duid`, reconciled to one row per interval and DUID, representing metered energy readings used as actual generation. `semantic.unit_dimension`, the current reconciled DUID dimension derived from MMSDM registration history, providing region, participant, station, fuel, registered capacity, storage flags, dispatch type, and emissions factor.

        ## Note

        Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

        ## Explanation

        Top row: REGIONID=TAS1, emissions_tco2e_proxy=0.0, generation_mwh=103768.48, tco2e_per_mwh=0.0. Returned 4 rows. The mean of `emissions_tco2e_proxy` across the result set is 16705.950.

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
    u.REGIONID,
    round(sum(g.MWH_READING * coalesce(u.CO2E_EMISSIONS_FACTOR, 0)), 2) AS emissions_tco2e_proxy,
    round(sum(g.MWH_READING), 2) AS generation_mwh,
    round(sum(g.MWH_READING * coalesce(u.CO2E_EMISSIONS_FACTOR, 0)) / nullIf(sum(g.MWH_READING), 0), 3) AS tco2e_per_mwh
FROM actual_gen g
INNER JOIN unit_dim u ON u.DUID = g.DUID
WHERE g.INTERVAL_DATETIME >= now() - INTERVAL 7 DAY
GROUP BY u.REGIONID
ORDER BY tco2e_per_mwh ASC
        ```
