SELECT d.transaction_type, COUNT(*) AS transactions, SUM(f.amount) AS total_amount,
       SUM(f.is_fraud) AS fraud_transactions,
       ROUND(100.0 * SUM(f.is_fraud) / NULLIF(COUNT(*), 0), 2) AS fraud_percentage
FROM fact_transaction f
JOIN dim_transaction_type d ON f.transaction_type_key = d.transaction_type_key
GROUP BY d.transaction_type
ORDER BY d.transaction_type;
