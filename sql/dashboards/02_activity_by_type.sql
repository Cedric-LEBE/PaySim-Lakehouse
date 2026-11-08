SELECT transaction_type, COUNT(*) AS transactions, SUM(amount) AS total_amount,
       SUM(is_fraud) AS fraud_transactions,
       ROUND(100.0 * SUM(is_fraud) / COUNT(*), 4) AS fraud_percentage
FROM {{STAR_VIEW}}
GROUP BY transaction_type;
