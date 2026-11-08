WITH filtered AS (
  SELECT * FROM transactions_dashboard
  WHERE (size(:types) = 0 OR array_contains(:types, transaction_type))
    AND (size(:fraud_labels) = 0 OR array_contains(:fraud_labels,
         CASE WHEN is_fraud = 1 THEN 'Fraudulent' ELSE 'Non-fraudulent' END))
    AND simulation_step BETWEEN try_cast(:hour_min AS BIGINT) AND try_cast(:hour_max AS BIGINT)
    AND amount BETWEEN try_cast(:amount_min AS DECIMAL(20,2)) AND try_cast(:amount_max AS DECIMAL(20,2))
)
