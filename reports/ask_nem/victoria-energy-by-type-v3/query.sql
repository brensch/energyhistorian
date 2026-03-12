SELECT
  toDate(d.SETTLEMENTDATE) AS day,
  ud.FUEL_TYPE AS fuel_type,
  sum(greatest(d.TOTALCLEARED, 0) / 12.0) AS energy_mwh
FROM semantic.daily_unit_dispatch AS d
JOIN semantic.unit_dimension AS ud USING (DUID)
WHERE ud.REGIONID = 'VIC1'
  AND toDate(d.SETTLEMENTDATE) BETWEEN today() - 6 AND today()
GROUP BY day, fuel_type
ORDER BY day DESC, fuel_type;