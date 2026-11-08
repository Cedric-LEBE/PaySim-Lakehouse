SELECT transaction_type, COUNT(*) AS transactions, SUM(amount) AS total_amount,
 AVG(amount) AS average_amount, percentile_approx(amount,0.5,10000) AS median_amount,
 SUM(is_fraud) AS fraud_transactions, AVG(is_fraud) AS fraud_rate,
 SUM(CASE WHEN is_fraud=1 THEN amount ELSE 0 END) AS fraud_amount
FROM filtered GROUP BY transaction_type ORDER BY transactions DESC
