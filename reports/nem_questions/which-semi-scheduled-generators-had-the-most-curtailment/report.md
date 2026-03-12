        # Which semi-scheduled generators had the most curtailment?

        - Status: `proxy`
        - Chart: `chart.png`
        - Raw results: `results.csv`

        ## Data

        This report is derived from the following semantic surfaces in ClickHouse: `semantic.dispatch_unit_solution`, reconciled to one row per settlement interval and DUID, providing the richer dispatch-solution surface including UIGF, storage fields, and semi-scheduled details. `semantic.unit_dimension`, the current reconciled DUID dimension derived from MMSDM registration history, providing region, participant, station, fuel, registered capacity, storage flags, dispatch type, and emissions factor.

        ## Note

        Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

        ## Explanation

        Top row: DUID=CUSF1, REGIONID=NSW1, FUEL_TYPE=Solar, curtailed_mwh_proxy=13612.22. Returned 25 rows. The mean of `curtailed_mwh_proxy` across the result set is 4590.420.

        ## SQL

        ```sql
        WITH

unit_solution AS (
    SELECT
        SETTLEMENTDATE,
        DUID,
        avg(TOTALCLEARED) AS TOTALCLEARED,
        avg(UIGF) AS UIGF
    FROM semantic.dispatch_unit_solution
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

SELECT
    s.DUID,
    any(u.REGIONID) AS REGIONID,
    any(u.FUEL_TYPE) AS FUEL_TYPE,
    round(sum(greatest(s.UIGF - s.TOTALCLEARED, 0) / 12.0), 2) AS curtailed_mwh_proxy
FROM unit_solution s
INNER JOIN unit_dim u ON u.DUID = s.DUID
WHERE coalesce(u.SCHEDULE_TYPE, '') = 'SEMI-SCHEDULED'
GROUP BY s.DUID
ORDER BY curtailed_mwh_proxy DESC
LIMIT 25
        ```
