# -*- coding: utf-8 -*-
"""
  @author: Brownbull - Gabriel Carcamo - carcamo.gabriel@gmail.com
  Format - Format Module inside Transform on ETL for houses model
"""
import pandas as pd
from include.logs import *
from include.files import *
from include.program import *
from main.ETL.stats import *

def format_portalinmobiliario(inDf, outCsvPath):
  # SET columns type
  inDf['PublishedDate'] = pd.to_datetime(inDf['PublishedDate'])
  inDf['Age'] = inDf['Age'].astype(int)
  inDf['CommonExpnsCLP'] = inDf['CommonExpnsCLP'].astype(int)
  inDf['Floor'] = inDf['Floor'].astype(int)
  inDf['MtTot'] = inDf['MtTot'].astype(int)
  inDf['MtUtil'] = inDf['MtUtil'].astype(int)
  inDf['Bdroom'] = inDf['Bdroom'].astype(int)
  inDf['Bath'] = inDf['Bath'].astype(int)
  inDf['Parking'] = inDf['Parking'].astype(int)
  inDf['Storage'] = inDf['Storage'].astype(int)
  inDf['Pool'] = inDf['Pool'].astype(int)
  inDf['PriceUF'] = inDf['PriceUF'].astype(int)
  # WRITE csv
  inDf.to_csv(outCsvPath, index=False)

# CSV processing
def formatCsv(srce, inCsvPath, outCsvPath):
  # Initial Vars
  inDf = pd.read_csv(inCsvPath)

  # SRCE switch
  if srce == "portal inmobiliario":
    format_portalinmobiliario(inDf, outCsvPath)
  else:
    print("srce {} is not supported".format(srce))
    exit()

# MAIN
def formatMain(log, snap, inCsvPaths, baseOutPath, statsPath, formatCols):
  # TIME start
  startTime, startStamp = getTimeAndStamp()
  # INIT
  currStep = "ETL_T_03_FORMAT"
  logPrint(log, "{} Start: {}".format(currStep, str(startStamp)))

  # SET
  outSnapPath = baseOutPath + "/" + snap
  statsSnapPath = statsPath + "/" + snap
  outFormatFiles = []

  ## iterate SOURCES
  for inCsvPath in inCsvPaths:
    srce = getRawFileName(inCsvPath)
    logPrint(log, "Processing Source: {}".format(inCsvPath))

    # OUTPUT setup
    outSnapStepPath = outSnapPath + "/" + currStep
    outCsvPath = initFilePath(outSnapStepPath, srce + ".csv")

    # WRITE header
    with open(outCsvPath, 'w', encoding="utf-8") as outCsv:
      outCsv.write(array2Str(formatCols, ',') + "\n")

    ## FORMAT
    formatCsv(
      srce, 
      inCsvPath, # inFile
      outCsvPath) # outFile

    outFormatFiles.append(outCsvPath)
  ## FINISH
  # END TIMING & LOG
  endTime, endStamp = getTimeAndStamp()
  logPrint(log, "{} End: {}".format(currStep, str(endStamp)))
  logPrint(log, "{} Total Execution Time: {}".format(currStep, str(endTime - startTime)))
  ## STATS
  statsSnapStepPath = statsSnapPath + "/" + currStep
  stepEnd(log, statsSnapStepPath, currStep, outFormatFiles)
  return outFormatFiles
