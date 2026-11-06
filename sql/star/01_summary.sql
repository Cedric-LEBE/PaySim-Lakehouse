SELECT COUNT(*) AS transactions,
       COALESCE(SUM(amount), CAST(0 AS DECIMAL(30,2))) AS total_amount,
       COALESCE(SUM(is_fraud), 0) AS fraud_transactions,
       COALESCE(SUM(CASE WHEN is_fraud = 1 THEN amount ELSE CAST(0 AS DECIMAL(20,2)) END),
                CAST(0 AS DECIMAL(30,2))) AS fraud_amount,
       ROUND(100.0 * SUM(is_fraud) / NULLIF(COUNT(*), 0), 2) AS fraud_percentage
FROM fact_transaction;
