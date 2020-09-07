# -*- coding: utf-8 -*-
"""
  @author: Brownbull - Gabriel Carcamo - carcamo.gabriel@gmail.com
  LookUp - LookUp Module inside Transform on ETL for houses model
"""
# IMPORT LIBRARIES
import pandas as pd
from main.include.logs import *
from main.include.files import *
from main.include.common import *
from main.include.program import *
from main.ETL.stats import *
from main.ETL.E.houses.selenium.portalinmboliarioItem import getFullItems


# MAIN
def lookupMain(log, snap, outCsvPath, statsPath, newLinksPath, outLookUpPath, chromeDriverPath, waitSeconds):
  dbg = False

  # TIME start
  startTime, startStamp = getTimeAndStamp()
  # INIT
  currStep = "ETL_E_02_LOOKUP"
  logPrint(log, "{} Start: {}".format(currStep, str(startStamp)))

  # SET
  outSnapPath = outCsvPath + "/" + snap + "/portalinmobiliario/" + currStep
  statsSnapPath = statsPath + "/" + snap
  # OUTPUT setup
  # initPath(outLookUpPath)
  outLookUpFiles = []

  # get Data
  logPrint(log, "Selenium to: {}".format(str(outLookUpPath)))
  outLookUpRows = getFullItems(chromeDriverPath, waitSeconds, newLinksPath, outLookUpPath)

  # return and Stats
  outLookUpFiles.append(outLookUpPath)
  lookUpTimeFile = timeCp(outLookUpPath, outSnapPath, ext = "csv")
  logPrint(log, "New time associated file: {}".format(lookUpTimeFile))

  ## FINISH
  # END TIMING & LOG
  endTime, endStamp = getTimeAndStamp()
  logPrint(log, "{} End: {}".format(currStep, str(endStamp)))
  logPrint(log, "{} Total Execution Time: {}".format(currStep, str(endTime - startTime)))
  ## STATS
  statsSnapStepPath = statsSnapPath + "/" + currStep
  stepEnd(log, statsSnapStepPath, currStep, outLookUpFiles)
  return outLookUpRows


