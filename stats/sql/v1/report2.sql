SELECT 
  "houses_v1"."SNAP_DT", 
  "houses_v1"."SRCE", 
  "houses_v1"."PROVINCE", 
  "houses_v1"."PROPERTY_TYPE", 
  count(*) as COUNT,
  ceil(AVG("houses_v1"."MT_TOT")) MT_AVG, 
  ceil(AVG("houses_v1"."PRICE_UF")) PRICE_AVG,
  ceil(AVG("housesplus"."uf_x_mt2")) UF_X_MT2_AVG,
  ceil(AVG("housesplus"."value_score")) VALUE_SCORE_AVG,
  ceil(AVG("housesplus"."score")) SCORE_AVG
FROM 
  houses_v1
INNER JOIN 
  housesplus
ON 
  "houses_v1"."ID" = "housesplus"."ID"
GROUP BY
  "houses_v1"."SNAP_DT", 
  "houses_v1"."SRCE", 
  "houses_v1"."PROVINCE", 
  "houses_v1"."PROPERTY_TYPE" 
ORDER BY
  SCORE_AVG DESC,
  UF_X_MT2_AVG 
  ;