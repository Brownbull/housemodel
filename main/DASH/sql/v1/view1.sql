SET search_path TO public;
DROP VIEW housesplus;
CREATE or replace VIEW housesplus AS
SELECT 
  "ID",
  "SNAP_DT", 
  "PROVINCE", 
  "PROPERTY_TYPE",
  ceil("PRICE_UF"::decimal / "MT_TOT") AS UF_X_MT2,
  CASE WHEN "PROPERTY_TYPE" = 'Apartment' THEN
    ceil(("BDROOM"*4)+("BATH"*4)+("PARKING"*2) * "MT_TOT"::decimal)
  ELSE
    ceil(("BDROOM"*4)+("BATH"*4)+("PARKING"*2) * "MT_TOT"::decimal * 0.7)
  END AS VALUE_SCORE,
  CASE WHEN "PROPERTY_TYPE" = 'Apartment' THEN
    ceil(((("BDROOM"*4)+("BATH"*4)+("PARKING"*2) * "MT_TOT"::decimal) *1000)/"PRICE_UF") 
  ELSE
    ceil(((("BDROOM"*4)+("BATH"*4)+("PARKING"*2) * "MT_TOT"::decimal * 0.7) *1000)/"PRICE_UF") 
  END AS SCORE
FROM houses_v1;