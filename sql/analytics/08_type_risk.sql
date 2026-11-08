SELECT transaction_type,COUNT(*) AS transactions,SUM(is_fraud) AS fraud_transactions,
 AVG(is_fraud) AS fraud_rate,AVG(is_fraud)/(SELECT NULLIF(AVG(is_fraud),0) FROM filtered) AS relative_rate,
 SUM(CASE WHEN is_fraud=1 THEN amount ELSE 0 END) AS fraud_amount
FROM filtered GROUP BY transaction_type ORDER BY fraud_transactions DESC
