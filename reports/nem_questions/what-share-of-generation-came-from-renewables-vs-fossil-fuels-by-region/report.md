        # What share of generation came from renewables vs fossil fuels by region?

        - Status: `ready`
        - Chart: `chart.png`
        - Raw results: `results.csv`

        ## Data

        This report is derived from the following semantic surfaces in ClickHouse: `semantic.actual_gen_duid`, reconciled to one row per interval and DUID, representing metered energy readings used as actual generation. `semantic.unit_dimension`, the current reconciled DUID dimension derived from MMSDM registration history, providing region, participant, station, fuel, registered capacity, storage flags, dispatch type, and emissions factor.

        ## Note

        Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

        ## Explanation

        Top row: REGIONID=NSW1, generation_group=Renewables, energy_mwh=209779.28, share_of_region=0.7531. Returned 6 rows. The mean of `energy_mwh` across the result set is 128271.372.

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

base AS (
    SELECT
        coalesce(u.REGIONID, 'UNKNOWN') AS REGIONID,

CASE
    WHEN lower(FUEL_TYPE) IN (
        'solar', 'wind', 'hydro', 'battery storage', 'bagasse',
        'biomass and industrial materials', 'other biofuels',
        'primary solid biomass fuels', 'landfill biogas methane'
    ) THEN 'Renewables'
    ELSE 'Fossil and other'
END
 AS generation_group,
        g.MWH_READING AS energy_mwh
    FROM actual_gen g
    LEFT JOIN unit_dim u ON u.DUID = g.DUID
    WHERE g.INTERVAL_DATETIME >= now() - INTERVAL 7 DAY
)

SELECT
    REGIONID,
    generation_group,
    round(energy_mwh, 2) AS energy_mwh,
    round(energy_mwh / sum(energy_mwh) OVER (PARTITION BY REGIONID), 4) AS share_of_region
FROM (
    SELECT
        REGIONID,
        generation_group,
        sum(energy_mwh) AS energy_mwh
    FROM base
    GROUP BY REGIONID, generation_group
)
ORDER BY REGIONID, share_of_region DESC
        ```
