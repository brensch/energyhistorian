SELECT
  toDate(a.INTERVAL_DATETIME) AS day,
  u.FUEL_TYPE AS type,
  sum(a.MWH_READING) AS energy_mwh
FROM semantic.actual_gen_duid AS a
JOIN semantic.unit_dimension AS u ON a.DUID = u.DUID
WHERE u.REGIONID = 'VIC1'
  AND a.INTERVAL_DATETIME >= now() - INTERVAL 7 DAY
GROUP BY day, type
ORDER BY day DESC, type
LIMIT 1000