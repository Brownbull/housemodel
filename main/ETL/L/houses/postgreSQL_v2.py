# -*- coding: utf-8 -*-
"""
  @author: Brownbull - Gabriel Carcamo - carcamo.gabriel@gmail.com
  Load Module inside Transform on ETL for houses model
"""
import pandas as pd
from main.include.logs import *
from main.include.files import *
from main.include.common import *
from main.include.program import *
from main.include.dbPostgreSQL import *
from main.ETL.stats import *

# CSV processing
def loadCsv(log, dbConn, inCsvPath, outCsvPath):
  default = "NULL"
  # Initial Vars
  inDf = pd.read_csv(inCsvPath)
  # WRITE
  ## CSV
  inDf.to_csv(outCsvPath, mode='a', header=False, index=False)
  logPrint(log, "loadCsv input: {}".format(inCsvPath))
  ## DB
  for idx, row in inDf.iterrows():
    insertSql = """ 
    INSERT INTO public.houses_v2(
	    "SNAP_DT", "SNAP_DT_TIME", "PUBLISHED_DT", "UPDATED_DT", "SRCE", "REGION", "PROVINCE", "PROPERTY_TYPE", "PROPERTY_STATE", "AGE", "STAGE", "DELIVERY", "COMMON_EXPNS_CLP", "FLOOR", "FOR_INVESTMENT", "SECTOR", "SIZE_GROUP", "MT_TOT", "MT_UTIL", "BDROOM", "BATH", "BALCONY", "PARKING", "STORAGE", "POOL", "PRICE_UF", "ETL_UF_X_MT2", "ETL_VALUE", "ETL_SCORE", "LINK")
	    VALUES ({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}, {13}, {14}, {15}, {16}, {17}, {18}, {19}, {20}, {21}, {22}, {23}, {24}, {25}, {26}, {27}, {28}, {29});""".format(
        row['SnapDate'] if checkIfexists('SnapDate', row) else default ,
        "\'" + fromCCYYMMDDtoDateStr(row['SnapDate']) + "\'" if checkIfexists('SnapDate', row) else default ,
        "\'" + row['PublishedDate'] + "\'" if checkIfexists('PublishedDate', row) else default ,
        "\'" + row['PublishedDate'] + "\'" if checkIfexists('PublishedDate', row) else default ,
        "\'" + row['Srce'] + "\'" if checkIfexists('Srce', row) else default ,
        "\'" + row['Region'] + "\'" if checkIfexists('Region', row) else default ,
        "\'" + row['Province'] + "\'" if checkIfexists('Province', row) else default ,
        "\'" + row['PropertyType'] + "\'" if checkIfexists('PropertyType', row) else default ,
        "\'" + row['PropertyState'] + "\'" if checkIfexists('PropertyState', row) else default ,
        row['Age'] if checkIfexists('Age', row) else default ,
        "\'" + row['Stage'] + "\'" if checkIfexists('Stage', row) else default ,
        "\'" + str(row['Delivery']) + "\'" if checkIfexists('Delivery', row) else default ,
        row['CommonExpnsCLP'] if checkIfexists('CommonExpnsCLP', row) else default ,
        row['Floor'] if checkIfexists('Floor', row) else default ,
        "\'" + row['ForInvestment'] + "\'" if checkIfexists('ForInvestment', row) else default ,
        row['Sector'] if checkIfexists('Sector', row) else default ,
        "\'" + row['SizeGroup'] + "\'" if checkIfexists('SizeGroup', row) else default ,
        row['MtTot'] if checkIfexists('MtTot', row) else default ,
        row['MtUtil'] if checkIfexists('MtUtil', row) else default ,
        row['Bdroom'] if checkIfexists('Bdroom', row) else default ,
        row['Bath'] if checkIfexists('Bath', row) else default ,
        row['Balcony'] if checkIfexists('Balcony', row) else default ,
        row['Parking'] if checkIfexists('Parking', row) else default ,
        row['Storage'] if checkIfexists('Storage', row) else default ,
        row['Pool'] if checkIfexists('Pool', row) else default ,
        row['PriceUF'] if checkIfexists('PriceUF', row) else default ,
        row['UFxMt2'] if checkIfexists('UFxMt2', row) else default ,
        row['Value'] if checkIfexists('Value', row) else default ,
        row['Score'] if checkIfexists('Score', row) else default ,
        "\'" + row['Link'] + "\'" if checkIfexists('Link', row) else default 
      )
    dbExec(log, dbConn, insertSql)

def updateSnap(log, dbConn, snap):
  snapUpdateQry = """ 
    UPDATE houses_v2 
    SET "UPDATED_DT"=NOW(), "STAGE"='out' 
    WHERE "LINK" IN 
      ( SELECT A."LINK"
        FROM      (SELECT * FROM houses_v2 WHERE "SNAP_DT" < {0}) A
        LEFT JOIN (SELECT * FROM houses_v2 WHERE "SNAP_DT" = {0}) B
        ON A."LINK" = B."LINK"
        WHERE b IS NULL);""".format(snap, snap)
  logPrint(log, "updateSnap executing: {}".format(snapUpdateQry))
  dbExec(log, dbConn, snapUpdateQry)

# MAIN
def loadMain(log, snap, inCsvPaths, baseOutPath, statsPath,  Cols, dbCfg):
  # TIME start
  startTime, startStamp = getTimeAndStamp()
  # INIT
  currStep = "ETL_L_00_LOAD"
  logPrint(log, "{} Start: {}".format(currStep, str(startStamp)))

  # SET
  outSnapPath = baseOutPath + "/" + snap
  statsSnapPath = statsPath + "/" + snap
  dbConn = dbGetConn(log, dbCfg['db'], dbCfg['user'], dbCfg['hst'], dbCfg['prt'], passPath=dbCfg['passPath'], keyPath=dbCfg['keyPath'])
  # OUTPUT setup
  outSnapStepPath = outSnapPath + "/" + currStep
  outCsvPath = initFilePath(outSnapStepPath, "allHouses.csv")

  # WRITE header
  with open(outCsvPath, 'w', encoding="utf-8") as outCsv:
    outCsv.write(array2Str(Cols, ',') + "\n")

  # CHECK table
  housesTableCreateSQL = """
    CREATE TABLE public.houses_v2
    (
      "ID" bigserial NOT NULL,
      "SNAP_DT" integer,
      "SNAP_DT_TIME" timestamp without time zone DEFAULT now(),
      "PUBLISHED_DT" date,
      "UPDATED_DT" date,
      "SRCE" character varying(50),
      "REGION" character varying(200),
      "PROVINCE" character varying(100),
      "PROPERTY_TYPE" character varying(20),
      "PROPERTY_STATE" character varying(10),
      "AGE" integer,
      "STAGE" character varying(50),
      "DELIVERY" character varying(100),
      "COMMON_EXPNS_CLP" integer,
      "FLOOR" integer,
      "FOR_INVESTMENT" character varying(3),
      "SECTOR" integer,
      "SIZE_GROUP" character varying(10),
      "MT_TOT" integer,
      "MT_UTIL" integer,
      "BDROOM" integer,
      "BATH" integer,
      "BALCONY" integer,
      "PARKING" integer,
      "STORAGE" integer,
      "POOL" integer,
      "PRICE_UF" integer,
      "ETL_UF_X_MT2" integer,
      "ETL_VALUE" integer,
      "ETL_SCORE" integer,
      "LINK" character varying(1000),
      PRIMARY KEY ("ID")
    )
    WITH (
      OIDS = FALSE
    );
    ALTER TABLE public.houses_v2
    OWNER to postgres;"""
  if not dbCheckTableExists(dbConn, "houses_v2"):
    logPrint(log, "houses_v2 table does not Exists")
    dbExec(log, dbConn, housesTableCreateSQL)
  else:
    logPrint(log, "houses_v2 table Exists")

  ## iterate SOURCES
  for inCsvPath in inCsvPaths:
    logPrint(log, "Processing Source: {}".format(inCsvPath))

    ## LOAD
    loadCsv(
      log, 
      dbConn,
      inCsvPath,
      outCsvPath)

    # UPDATE SNAP
    updateSnap(
      log, 
      dbConn,
      snap)

  dbEnd(log, dbConn)
  ## FINISH
  # END TIMING & LOG
  endTime, endStamp = getTimeAndStamp()
  logPrint(log, "{} End: {}".format(currStep, str(endStamp)))
  logPrint(log, "{} Total Execution Time: {}".format(currStep, str(endTime - startTime)))
  # ## STATS
  # statsSnapStepPath = statsSnapPath + "/" + currStep
  # stepEnd(log, statsSnapStepPath, currStep, outFillFiles)
