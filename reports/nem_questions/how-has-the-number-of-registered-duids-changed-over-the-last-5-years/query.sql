
SELECT
    toStartOfYear(START_DATE) AS year_start,
    countDistinct(DUID) AS registered_duids
FROM semantic.participant_registration_dudetailsummary
WHERE START_DATE >= now() - INTERVAL 5 YEAR
GROUP BY year_start
ORDER BY year_start