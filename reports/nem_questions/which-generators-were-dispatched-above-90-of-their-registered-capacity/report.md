        # Which generators were dispatched above 90% of their registered capacity?

        - Status: `ready`
        - Chart: `chart.png`
        - Raw results: `results.csv`

        ## Data

        This report is derived from the following semantic surfaces in ClickHouse: `semantic.daily_unit_dispatch`, reconciled to one row per settlement interval and DUID, providing dispatch targets, initial MW, availability, ramp rates, and FCAS enablement. `semantic.unit_dimension`, the current reconciled DUID dimension derived from MMSDM registration history, providing region, participant, station, fuel, registered capacity, storage flags, dispatch type, and emissions factor.

        ## Note

        Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

        ## Explanation

        Top row: DUID=GANNSF1, REGIONID=VIC1, FUEL_TYPE=Solar, registered_capacity_mw=1.0. Returned 50 rows. The mean of `registered_capacity_mw` across the result set is 200.980.

        ## SQL

        ```sql
        WITH

unit_dispatch AS (
    SELECT
        SETTLEMENTDATE,
        DUID,
        avg(TOTALCLEARED) AS TOTALCLEARED,
        avg(INITIALMW) AS INITIALMW,
        avg(AVAILABILITY) AS AVAILABILITY,
        avg(RAMPUPRATE) AS RAMPUPRATE,
        avg(RAMPDOWNRATE) AS RAMPDOWNRATE,
        avg(RAISE6SEC + RAISE60SEC + RAISE5MIN + RAISEREG) AS FCAS_RAISE_TOTAL,
        avg(LOWER6SEC + LOWER60SEC + LOWER5MIN + LOWERREG) AS FCAS_LOWER_TOTAL
    FROM semantic.daily_unit_dispatch
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
    d.DUID,
    any(u.REGIONID) AS REGIONID,
    any(u.FUEL_TYPE) AS FUEL_TYPE,
    any(u.REGISTEREDCAPACITY_MW) AS registered_capacity_mw,
    max(d.TOTALCLEARED) AS max_dispatch_mw,
    round(max(d.TOTALCLEARED) / nullIf(any(u.REGISTEREDCAPACITY_MW), 0), 3) AS max_capacity_factor
FROM unit_dispatch d
INNER JOIN unit_dim u ON u.DUID = d.DUID
WHERE coalesce(u.DISPATCHTYPE, '') = 'GENERATOR'
GROUP BY d.DUID
HAVING max_dispatch_mw >= 0.9 * registered_capacity_mw
ORDER BY max_capacity_factor DESC
LIMIT 50
        ```
