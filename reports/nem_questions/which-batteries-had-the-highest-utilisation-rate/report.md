        # Which batteries had the highest utilisation rate?

        - Status: `proxy`
        - Chart: `chart.png`
        - Raw results: `results.csv`

        ## Data

        This report is derived from the following semantic surfaces in ClickHouse: `semantic.daily_unit_dispatch`, reconciled to one row per settlement interval and DUID, providing dispatch targets, initial MW, availability, ramp rates, and FCAS enablement. `semantic.unit_dimension`, the current reconciled DUID dimension derived from MMSDM registration history, providing region, participant, station, fuel, registered capacity, storage flags, dispatch type, and emissions factor.

        ## Note

        Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

        ## Explanation

        Top row: DUID=MREHA3, REGIONID=VIC1, max_capacity_mw=200.0, avg_abs_dispatch_mw=61.1. Returned 20 rows. The mean of `max_capacity_mw` across the result set is 148.200.

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
    any(u.MAXCAPACITY_MW) AS max_capacity_mw,
    round(avg(abs(d.TOTALCLEARED)), 2) AS avg_abs_dispatch_mw,
    round(avg(abs(d.TOTALCLEARED)) / nullIf(any(u.MAXCAPACITY_MW), 0), 3) AS utilisation_rate
FROM unit_dispatch d
INNER JOIN unit_dim u ON u.DUID = d.DUID
WHERE u.IS_STORAGE = 1
GROUP BY d.DUID
ORDER BY utilisation_rate DESC
LIMIT 20
        ```
