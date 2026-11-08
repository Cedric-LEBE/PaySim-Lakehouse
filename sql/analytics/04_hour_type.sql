SELECT simulation_step, transaction_type, COUNT(*) AS transactions,
 SUM(amount) AS total_amount, SUM(is_fraud) AS fraud_transactions
FROM filtered GROUP BY simulation_step,transaction_type ORDER BY simulation_step,transaction_type
