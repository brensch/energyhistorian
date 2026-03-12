        # What was the SCADA output profile of the top 10 generators over the last day?

        - Status: `ready`
        - Chart: `chart.png`
        - Raw results: `results.csv`

        ## Data

        This report is derived from the following semantic surfaces in ClickHouse: `semantic.dispatch_unit_scada`, reconciled to one row per settlement interval and DUID, representing SCADA telemetry values. `semantic.unit_dimension`, the current reconciled DUID dimension derived from MMSDM registration history, providing region, participant, station, fuel, registered capacity, storage flags, dispatch type, and emissions factor.

        ## Note

        Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

        ## Explanation

        Top row: SETTLEMENTDATE=2026-03-12 14:10:00, DUID=BW03, FUEL_TYPE=Black coal, scada_mw=449.67728. Returned 300 rows. The mean of `scada_mw` across the result set is 407.115.

        ## SQL

        ```sql
        WITH

unit_scada AS (
    SELECT
        SETTLEMENTDATE,
        DUID,
        avg(SCADAVALUE) AS SCADAVALUE
    FROM semantic.dispatch_unit_scada
    GROUP BY SETTLEMENTDATE, DUID
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

top_generators AS (
    SELECT DUID
    FROM (
        SELECT DUID, avg(SCADAVALUE) AS avg_scada
        FROM unit_scada
        WHERE SETTLEMENTDATE >= now() - INTERVAL 1 DAY
        GROUP BY DUID
        ORDER BY avg_scada DESC
        LIMIT 10
    )
)

SELECT
    s.SETTLEMENTDATE,
    s.DUID,
    any(u.FUEL_TYPE) AS FUEL_TYPE,
    avg(s.SCADAVALUE) AS scada_mw
FROM unit_scada s
LEFT JOIN unit_dim u ON u.DUID = s.DUID
WHERE s.SETTLEMENTDATE >= now() - INTERVAL 1 DAY
  AND s.DUID IN (SELECT DUID FROM top_generators)
GROUP BY s.SETTLEMENTDATE, s.DUID
ORDER BY s.SETTLEMENTDATE, s.DUID
        ```
