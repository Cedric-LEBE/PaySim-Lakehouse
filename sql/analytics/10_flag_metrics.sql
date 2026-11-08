SELECT tp,fp,fn,tn,
 tp*1.0/NULLIF(tp+fp,0) AS precision_flag,
 tp*1.0/NULLIF(tp+fn,0) AS recall_flag,
 2.0*tp/NULLIF(2*tp+fp+fn,0) AS f1_flag,
 (tp+tn)*1.0/NULLIF(tp+fp+fn+tn,0) AS accuracy_flag
FROM (SELECT COALESCE(SUM(CASE WHEN is_fraud=1 AND is_flagged_fraud=1 THEN 1 ELSE 0 END),0) AS tp,
 COALESCE(SUM(CASE WHEN is_fraud=0 AND is_flagged_fraud=1 THEN 1 ELSE 0 END),0) AS fp,
 COALESCE(SUM(CASE WHEN is_fraud=1 AND is_flagged_fraud=0 THEN 1 ELSE 0 END),0) AS fn,
 COALESCE(SUM(CASE WHEN is_fraud=0 AND is_flagged_fraud=0 THEN 1 ELSE 0 END),0) AS tn FROM filtered)
