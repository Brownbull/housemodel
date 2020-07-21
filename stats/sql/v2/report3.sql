SELECT 
  t.SNAP_DT, 
  t.SRCE, 
  t.REGION, 
  t.PROVINCE, 
  t.PROPERTY_TYPE, 
  t.PROPERTY_STATE, 
  t.SIZE_GROUP, 
  count(*) as COUNT,
  ceil(AVG(t.MT_TOT)) MT_TOT_AVG, 
  ceil(AVG(t.MT_UTIL)) MT_UTIL_AVG, 
  ceil(AVG(t.PRICE_UF)) PRICE_AVG,
  ceil(AVG(t.ETL_UF_X_MT2)) ETL_UF_X_MT2_AVG,
  ceil(AVG(t.ETL_VALUE)) ETL_VALUE_AVG,
  ceil(AVG(t.ETL_SCORE)) ETL_SCORE_AVG
FROM (
  SELECT
    "houses_v2"."SNAP_DT" as SNAP_DT, 
    "houses_v2"."SRCE" as SRCE, 
    "houses_v2"."REGION" as REGION, 
    "houses_v2"."PROVINCE" as PROVINCE, 
    "houses_v2"."PROPERTY_TYPE" as PROPERTY_TYPE, 
    "houses_v2"."PROPERTY_STATE" as PROPERTY_STATE, 
    "houses_v2"."SIZE_GROUP" as SIZE_GROUP, 
    NULLIF("houses_v2"."MT_TOT" , 16) as MT_TOT , 
    NULLIF("houses_v2"."MT_UTIL" , 16) as MT_UTIL , 
    NULLIF("houses_v2"."PRICE_UF" , 16) as PRICE_UF ,
    NULLIF("houses_v2"."ETL_UF_X_MT2" , 16) as ETL_UF_X_MT2 ,
    NULLIF("houses_v2"."ETL_VALUE" , 16) as ETL_VALUE ,
    NULLIF("houses_v2"."ETL_SCORE" , 16) as ETL_SCORE 
  FROM
    houses_v2) t
GROUP BY
  t.SNAP_DT, 
  t.SRCE, 
  t.REGION, 
  t.PROVINCE, 
  t.PROPERTY_TYPE, 
  t.PROPERTY_STATE,
  t.SIZE_GROUP
ORDER BY
  ETL_SCORE_AVG DESC,
  ETL_UF_X_MT2_AVG 
  ;