SELECT f.transaction_key, o.account_id AS origin_account, o.account_kind AS origin_kind,
       d.account_id AS destination_account, d.account_kind AS destination_kind,
       f.amount, f.origin_balance_before, f.origin_balance_after,
       f.destination_balance_before, f.destination_balance_after
FROM fact_transaction f
JOIN dim_account o ON f.origin_account_key = o.account_key
JOIN dim_account d ON f.destination_account_key = d.account_key
WHERE f.is_fraud = 1
ORDER BY f.transaction_key;
