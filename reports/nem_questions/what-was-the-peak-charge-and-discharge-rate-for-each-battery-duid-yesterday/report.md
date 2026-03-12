        # What was the peak charge and discharge rate for each battery DUID yesterday?

        - Status: `proxy`
        - Chart: `chart.png`
        - Raw results: `results.csv`

        ## Data

        This report is derived from the following semantic surfaces in ClickHouse: `semantic.daily_unit_dispatch`, reconciled to one row per settlement interval and DUID, providing dispatch targets, initial MW, availability, ramp rates, and FCAS enablement. `semantic.unit_dimension`, the current reconciled DUID dimension derived from MMSDM registration history, providing region, participant, station, fuel, registered capacity, storage flags, dispatch type, and emissions factor.

        ## Note

        Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

        ## Explanation

        Top row: DUID=ERB01, REGIONID=NSW1, peak_discharge_mw=459.98, peak_charge_mw=460.0. Returned 57 rows. The mean of `peak_discharge_mw` across the result set is 55.687.

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
    round(max(greatest(d.TOTALCLEARED, 0)), 2) AS peak_discharge_mw,
    round(max(abs(least(d.TOTALCLEARED, 0))), 2) AS peak_charge_mw
FROM unit_dispatch d
INNER JOIN unit_dim u ON u.DUID = d.DUID
WHERE toDate(d.SETTLEMENTDATE) = yesterday()
  AND u.IS_STORAGE = 1
GROUP BY d.DUID
ORDER BY peak_discharge_mw DESC, peak_charge_mw DESC
        ```
