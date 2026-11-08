SELECT simulation_step, COUNT(*) AS transactions, SUM(amount) AS total_amount,
 AVG(amount) AS average_amount, SUM(is_fraud) AS fraud_transactions, AVG(is_fraud) AS fraud_rate,
 SUM(CASE WHEN is_fraud=1 THEN amount ELSE 0 END) AS fraud_amount
FROM filtered GROUP BY simulation_step ORDER BY simulation_step
