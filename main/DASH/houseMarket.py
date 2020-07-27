# -*- coding: utf-8 -*-
"""
  @author: Brownbull - Gabriel Carcamo - carcamo.gabriel@gmail.com
  hoseMarket - Group Step Module inside Transform on DASH for houses model
"""
import pandas as pd
from include.logs import *
from include.files import *
from include.common import *
from include.program import *
from include.dbPostgreSQL import *
# from main.DASH.stats import *


def groupHouseMarket( log, createMarketView, selectMarketView, baseOutPath, statsPath, dbCfg):
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
  houseMarketPath = initFilePath(outStepPath, "houseMarket.csv")

  ## CHECK table
  if not dbCheckTableExists(dbConn, baseTable):
    logPrint(log, "{} table does not Exists, FAILED.".format(baseTable))
    exit()
  else:
    logPrint(log, "{} table does Exists, OK.".format(baseTable))

  if not dbCheckViewExists(dbConn, marketView):
    logPrint(log, "{} table does not Exists, Creating.".format(marketView))
    dbExecFile(log, dbConn, createMarketView)
    # dbConn.execute(open(createMarketView, "r").read())
  else:
    logPrint(log, "{} table does Exists, OK.".format(marketView))

  # SAVE 
  dbExecFileToCSV(log, dbConn, selectMarketView, houseMarketPath)

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