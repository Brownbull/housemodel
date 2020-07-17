SELECT 
  "houses"."SNAP_DT", 
  "houses"."PROVINCE", 
  "houses"."PROPERTY_TYPE", 
  ceil(AVG("houses"."MT_TOT")) MT_AVG, 
  ceil(AVG("houses"."PRICE_UF")) PRICE_AVG,
  ceil(AVG("housesplus"."uf_x_mt2")) UF_X_MT2_AVG,
  ceil(AVG("housesplus"."value_score")) VALUE_SCORE_AVG,
  ceil(AVG("housesplus"."score")) SCORE_AVG
FROM 
  houses
INNER JOIN 
  housesplus
ON 
  "houses"."ID" = "housesplus"."ID"
GROUP BY
  "houses"."SNAP_DT", 
  "houses"."PROVINCE", 
  "houses"."PROPERTY_TYPE" 
ORDER BY
  SCORE_AVG DESC,
  UF_X_MT2_AVG 
  ;