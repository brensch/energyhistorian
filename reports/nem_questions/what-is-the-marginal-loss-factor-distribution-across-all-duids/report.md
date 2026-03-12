        # What is the marginal loss factor distribution across all DUIDs?

        - Status: `ready`
        - Chart: `chart.png`
        - Raw results: `results.csv`

        ## Data

        This report is derived from the following semantic surface in ClickHouse: `semantic.marginal_loss_factors`, a NEMWEB-derived reference view containing transmission loss factors by DUID and connection point.

        ## Note

        Results are generated from the current semantic layer in ClickHouse. If the warehouse does not yet contain full market history, the answer reflects the available window rather than authoritative all-time history.

        ## Explanation

        Top row: DUID=, MLF=0.7774, REGIONID=NSW1. Returned 245,963 rows. The mean of `MLF` across the result set is 0.970.

        ## SQL

        ```sql

SELECT
    DUID,
    TRANSMISSIONLOSSFACTOR AS MLF,
    REGIONID
FROM semantic.marginal_loss_factors
ORDER BY TRANSMISSIONLOSSFACTOR
        ```
