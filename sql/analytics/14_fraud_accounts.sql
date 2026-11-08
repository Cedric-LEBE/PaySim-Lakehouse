SELECT account_role,account,COUNT(*) AS fraud_participations,SUM(amount) AS fraud_amount
FROM (SELECT 'Origin Account' AS account_role,origin_account AS account,amount FROM filtered WHERE is_fraud=1
 UNION ALL SELECT 'Destination Account',destination_account,amount FROM filtered WHERE is_fraud=1)
GROUP BY account_role,account ORDER BY fraud_amount DESC,account_role,account LIMIT 30
