# -*- coding: utf-8 -*-
"""
  @author: Brownbull - Gabriel Carcamo - carcamo.gabriel@gmail.com
  hoseMarket - Group Step Module inside Transform on DASH for houses model
"""
import pandas as pd
from main.include.logs import *
from main.include.files import *
from main.include.common import *
from main.include.program import *
from main.include.dbPostgreSQL import *
# from main.DASH.stats import *


def groupHouseMarket( log, selectBaseTable, createMarketView, selectMarketView, baseOutPath, statsPath, dbCfg):
  baseTable = "houses_v2"
  marketView = "house_market_v2"
  # TIME start
  startTime, startStamp = getTimeAndStamp()
  # INIT
  currStep = "DASH_01_GROUP"
  logPrint(log, "{} Start: {}".format(currStep, str(startStamp)))

  # SET
  outStepPath = baseOutPath + "/" + currStep
  statsStepPath = statsPath + "/" + currStep
  dbConn = dbGetConn(log, dbCfg['db'], dbCfg['user'], dbCfg['hst'], dbCfg['prt'])

  # OUTPUT setup
  baseTablePath = initFilePath(outStepPath, baseTable + ".csv")
  houseMarketPath = initFilePath(outStepPath, marketView + ".csv")

  ## CHECK table
  if not dbCheckTableExists(dbConn, baseTable):
    logPrint(log, "{} table does not Exists, FAILED.".format(baseTable))
    exit()
  else:
    logPrint(log, "{} table does Exists, OK.".format(baseTable))

  if not dbCheckViewExists(dbConn, marketView):
    logPrint(log, "{} view does not Exists, Creating.".format(marketView))
  else:
    logPrint(log, "{} view does Exists, Redefining.".format(marketView))
    dbDropView(log, dbConn, marketView)
  dbExecFile(log, dbConn, createMarketView)

  # SAVE 
  dbExecFileToCSV(log, dbConn, selectMarketView, houseMarketPath)
  saveBasetoCsv = input("Save base table {} on {} file? y/n\t".format(baseTable, baseTablePath))
  if saveBasetoCsv.lower() == 'y':
    logPrint(log, "Saving base table {} on {}".format(baseTable, baseTablePath))
    dbExecFileToCSV(log, dbConn, selectBaseTable, baseTablePath)

  dbEnd(log, dbConn)
  ## FINISH
  # END TIMING & LOG
  endTime, endStamp = getTimeAndStamp()
  logPrint(log, "{} End: {}".format(currStep, str(endStamp)))
  logPrint(log, "{} Total Execution Time: {}".format(currStep, str(endTime - startTime)))
  # ## STATS
  # statsSnapStepPath = statsSnapPath + "/" + currStep
  # stepEnd(log, statsSnapStepPath, currStep, outGroupFiles)
  return houseMarketPath