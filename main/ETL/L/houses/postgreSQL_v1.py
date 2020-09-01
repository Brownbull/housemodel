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
  ## DB
  for idx, row in inDf.iterrows():
    insertSql = """ 
    INSERT INTO public.houses(
	    "SNAP_DT", "SRCE", "PROVINCE", "PUBLISHED_DT", "PUBLISHED_TIMESMTP", "PROPERTY_TYPE", "PROPERTY_STATE", "BDROOM", "BATH", "PARKING", "POOL", "MT_UTIL", "MT_TOT", "PRICE_UF", "LINK")
	    VALUES ({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}, {13}, {14});""".format(
        row['SnapDate'] if checkIfexists('SnapDate', row) else default ,
        "\'" + row['Srce'] + "\'" if checkIfexists('Srce', row) else default ,
        "\'" + row['Province'] + "\'" if checkIfexists('Province', row) else default ,
        "\'" + row['PublishedDate'] + "\'" if checkIfexists('PublishedDate', row) else default ,
        "\'" + row['PublishedDate'] + "\'" if checkIfexists('PublishedDate', row) else default ,
        "\'" + row['PropertyType'] + "\'" if checkIfexists('PropertyType', row) else default ,
        "\'" + row['PropertyState'] + "\'" if checkIfexists('PropertyState', row) else default ,
        row['Bdroom'] if checkIfexists('Bdroom', row) else default ,
        row['Bath'] if checkIfexists('Bath', row) else default ,
        row['Parking'] if checkIfexists('Parking', row) else default ,
        row['Pool'] if checkIfexists('Pool', row) else default ,
        row['MtUtil'] if checkIfexists('MtUtil', row) else default ,
        row['MtTot'] if checkIfexists('MtTot', row) else default ,
        row['PriceUF'] if checkIfexists('PriceUF', row) else default ,
        "\'" + row['Link'] + "\'" if checkIfexists('Link', row) else default 
      )
    dbExec(log, dbConn, insertSql)
    

# MAIN
def loadMain(log, baseOutPath, statsPath, snap, inCsvPaths, Cols, dbCfg):
  # TIME start
  startTime, startStamp = getTimeAndStamp()
  # INIT
  currStep = "ETL_05_LOAD"
  logPrint(log, "{} Start: {}".format(currStep, str(startStamp)))

  # SET
  outSnapPath = baseOutPath + "/" + snap
  statsSnapPath = statsPath + "/" + snap
  dbConn = dbGetConn(log, dbCfg['db'], dbCfg['user'], dbCfg['hst'], dbCfg['prt'])

  # OUTPUT setup
  outSnapStepPath = outSnapPath + "/" + currStep
  outCsvPath = initFilePath(outSnapStepPath, "allHouses.csv")

  # WRITE header
  with open(outCsvPath, 'w', encoding="utf-8") as outCsv:
    outCsv.write(array2Str(Cols, ',') + "\n")

  # CHECK table
  housesTableCreateSQL = """
    CREATE TABLE public.houses
    (
      "ID" bigserial NOT NULL,
      "SNAP_DT" integer,
      "SRCE" character varying(50),
      "PROVINCE" character varying(100),
      "PUBLISHED_DT" date,
      "PUBLISHED_TIMESMTP" timestamp without time zone DEFAULT now(),
      "PROPERTY_TYPE" character varying(20),
      "PROPERTY_STATE" character varying(10),
      "BDROOM" integer,
      "BATH" integer,
      "PARKING" integer,
      "POOL" integer,
      "MT_UTIL" integer,
      "MT_TOT" integer,
      "PRICE_UF" integer,
      "LINK" character varying(1000),
      PRIMARY KEY ("ID")
    )
    WITH (
      OIDS = FALSE
    );
    ALTER TABLE public.houses
    OWNER to postgres;"""
  if not dbCheckTableExists(dbConn, "houses"):
    logPrint(log, "houses table does not Exists")
    dbExec(log, dbConn, housesTableCreateSQL)
  else:
    logPrint(log, "houses table Exists")

  ## iterate SOURCES
  for inCsvPath in inCsvPaths:
    srce = getRawFileName(inCsvPath)
    logPrint(log, "Processing Source: {}".format(inCsvPath))

    ## FORMAT
    loadCsv(
      log, 
      dbConn,
      inCsvPath,
      outCsvPath)

  dbEnd(log, dbConn)
  ## FINISH
  # END TIMING & LOG
  endTime, endStamp = getTimeAndStamp()
  logPrint(log, "{} End: {}".format(currStep, str(endStamp)))
  logPrint(log, "{} Total Execution Time: {}".format(currStep, str(endTime - startTime)))
  # ## STATS
  # statsSnapStepPath = statsSnapPath + "/" + currStep
  # stepEnd(log, statsSnapStepPath, currStep, outFillFiles)
