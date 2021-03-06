UPDATE houses_v2 
SET "UPDATED_DT"=NOW(), "STAGE"='out' 
WHERE
  "LINK" IN 
(SELECT
  A."LINK"
FROM
    (SELECT * FROM houses_v2 WHERE "SNAP_DT" < 20200718) A
LEFT JOIN 
  (SELECT * FROM houses_v2 WHERE "SNAP_DT" = 20200718) B
   ON A."LINK" = B."LINK"
WHERE b IS NULL);

SELECT "UPDATED_DT", "STAGE", "LINK" 
FROM houses_v2 
WHERE "SNAP_DT" = 20200718 AND "STAGE"='out';

SELECT "SNAP_DT", COUNT(*)
FROM houses_v2 
WHERE "STAGE"='out'
GROUP BY "SNAP_DT";


