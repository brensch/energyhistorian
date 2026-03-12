SELECT
  u.DUID,
  u.STATIONNAME,
  u.PARTICIPANTID,
  sum(greatest(d.TOTALCLEARED, 0) / 12.0 * p.RRP) AS estimated_revenue,
  sum(greatest(d.TOTALCLEARED, 0) / 12.0) AS estimated_MWh
FROM semantic.daily_unit_dispatch AS d
JOIN semantic.unit_dimension AS u ON d.DUID = u.DUID
JOIN semantic.dispatch_price AS p ON d.SETTLEMENTDATE = p.SETTLEMENTDATE AND u.REGIONID = p.REGIONID
WHERE d.SETTLEMENTDATE >= now() - INTERVAL 7 DAY
GROUP BY u.DUID, u.STATIONNAME, u.PARTICIPANTID
ORDER BY estimated_revenue DESC
LIMIT 10