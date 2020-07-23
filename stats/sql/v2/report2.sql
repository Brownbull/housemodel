SELECT 
  t.SNAP_DT, 
  t.SRCE, 
  t.REGION, 
  t.PROVINCE, 
  t.PROPERTY_TYPE, 
  t.PROPERTY_STATE, 
  t.SIZE_GROUP, 
  count(*) as COUNT,
  ROUND(corr(MT_UTIL, PRICE_UF)::numeric, 2) CORR_MTUTIL_PRICE_UF
FROM (
  SELECT
    "houses_v2"."SNAP_DT" as SNAP_DT, 
    "houses_v2"."SRCE" as SRCE, 
    "houses_v2"."REGION" as REGION, 
    "houses_v2"."PROVINCE" as PROVINCE, 
    "houses_v2"."PROPERTY_TYPE" as PROPERTY_TYPE, 
    "houses_v2"."PROPERTY_STATE" as PROPERTY_STATE, 
    "houses_v2"."SIZE_GROUP" as SIZE_GROUP, 
    NULLIF("houses_v2"."MT_TOT" , -16) as MT_TOT , 
    NULLIF("houses_v2"."MT_UTIL" , -16) as MT_UTIL , 
    NULLIF("houses_v2"."PRICE_UF" , -16) as PRICE_UF ,
    NULLIF("houses_v2"."ETL_UF_X_MT2" , -16) as ETL_UF_X_MT2 ,
    NULLIF("houses_v2"."ETL_VALUE" , -16) as ETL_VALUE ,
    NULLIF("houses_v2"."ETL_SCORE" , -16) as ETL_SCORE 
  FROM
    houses_v2) t
WHERE 
  t.SNAP_DT = 20200721
GROUP BY
  t.SNAP_DT, 
  t.SRCE, 
  t.REGION, 
  t.PROVINCE, 
  t.PROPERTY_TYPE, 
  t.PROPERTY_STATE,
  t.SIZE_GROUP
ORDER BY
  T.PROVINCE
  ;