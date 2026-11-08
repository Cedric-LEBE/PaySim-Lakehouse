SELECT is_fraud,is_flagged_fraud,COUNT(*) AS transactions,
 CASE WHEN is_fraud=1 AND is_flagged_fraud=1 THEN 'True Positive'
 WHEN is_fraud=0 AND is_flagged_fraud=1 THEN 'False Positive'
 WHEN is_fraud=1 AND is_flagged_fraud=0 THEN 'False Negative' ELSE 'True Negative' END AS outcome
FROM filtered GROUP BY is_fraud,is_flagged_fraud ORDER BY is_fraud,is_flagged_fraud
