SELECT
  PARTICIPANTID,
  sum(REGISTEREDCAPACITY_MW) AS total_registered_capacity_mw,
  count(DUID) AS duid_count
FROM semantic.unit_dimension
WHERE PARTICIPANTID IS NOT NULL
GROUP BY PARTICIPANTID
ORDER BY total_registered_capacity_mw DESC
LIMIT 20;