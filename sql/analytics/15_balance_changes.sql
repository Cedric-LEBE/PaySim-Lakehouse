SELECT transaction_type,CASE WHEN is_fraud=1 THEN 'Fraudulent' ELSE 'Non-fraudulent' END AS fraud_label,
 COUNT(*) AS transactions,AVG(amount) AS average_amount,
 AVG(origin_balance_after-origin_balance_before) AS average_origin_change,
 AVG(destination_balance_after-destination_balance_before) AS average_destination_change,
 SUM(CASE WHEN origin_balance_after=0 THEN 1 ELSE 0 END) AS origin_zero_after
FROM filtered GROUP BY transaction_type,is_fraud ORDER BY transaction_type,is_fraud
