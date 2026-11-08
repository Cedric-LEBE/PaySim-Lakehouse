SELECT CASE WHEN is_fraud=1 THEN 'Fraudulent' ELSE 'Non-fraudulent' END AS fraud_label,
 COUNT(*) AS transactions,SUM(amount) AS total_amount,AVG(amount) AS average_amount,
 percentile_approx(amount,0.5,10000) AS median_amount,
 percentile_approx(amount,0.9,10000) AS p90_amount,
 percentile_approx(amount,0.99,10000) AS p99_amount,MAX(amount) AS maximum_amount
FROM filtered GROUP BY is_fraud ORDER BY is_fraud
