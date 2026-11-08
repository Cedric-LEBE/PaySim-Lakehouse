SELECT COUNT(*) AS transactions,
       COALESCE(SUM(amount), 0) AS total_amount,
       AVG(amount) AS average_amount,
       COALESCE(SUM(is_fraud), 0) AS fraud_transactions,
       ROUND(100.0 * SUM(is_fraud) / NULLIF(COUNT(*), 0), 4) AS fraud_percentage,
       COALESCE(SUM(CASE WHEN is_fraud = 1 THEN amount ELSE 0 END), 0) AS fraud_amount
FROM {{STAR_VIEW}};
