SELECT amount_band, MIN(band_order) AS band_order, COUNT(*) AS transactions,
 SUM(amount) AS total_amount, SUM(is_fraud) AS fraud_transactions, AVG(is_fraud) AS fraud_rate
FROM (SELECT *, CASE WHEN amount=0 THEN '0' WHEN amount<1000 THEN 'Over 0 to <1,000'
 WHEN amount<10000 THEN '1,000 to <10,000' WHEN amount<100000 THEN '10,000 to <100,000'
 WHEN amount<1000000 THEN '100,000 to <1,000,000' ELSE '1,000,000+' END AS amount_band,
 CASE WHEN amount=0 THEN 0 WHEN amount<1000 THEN 1 WHEN amount<10000 THEN 2
 WHEN amount<100000 THEN 3 WHEN amount<1000000 THEN 4 ELSE 5 END AS band_order FROM filtered)
GROUP BY amount_band ORDER BY band_order
