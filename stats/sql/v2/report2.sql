SELECT 
  "houses_v2"."SNAP_DT", 
  "houses_v2"."SRCE", 
  "houses_v2"."PROVINCE", 
  "houses_v2"."PROPERTY_TYPE", 
  count(*) as COUNT,
  ceil(AVG("houses_v2"."MT_TOT")) MT_AVG, 
  ceil(AVG("houses_v2"."PRICE_UF")) PRICE_AVG,
  ceil(AVG("housesplus_v2"."uf_x_mt2")) UF_X_MT2_AVG,
  ceil(AVG("housesplus_v2"."value_score")) VALUE_SCORE_AVG,
  ceil(AVG("housesplus_v2"."score")) SCORE_AVG
FROM 
  houses_v2
INNER JOIN 
  housesplus_v2
ON 
  "houses_v2"."ID" = "housesplus_v2"."ID"
GROUP BY
  "houses_v2"."SNAP_DT", 
  "houses_v2"."SRCE", 
  "houses_v2"."PROVINCE", 
  "houses_v2"."PROPERTY_TYPE" 
ORDER BY
  SCORE_AVG DESC,
  UF_X_MT2_AVG 
  ;