# -*- coding: utf-8 -*-
"""
  @author: Brownbull - Gabriel Carcamo - carcamo.gabriel@gmail.com
  Feature Engineering Module inside Transform on ETL for houses model
"""
import pandas as pd
from include.logs import *
from include.files import *
from include.program import *
from main.ETL.stats import *

global PublishedDateLastMode

# CSV processing
def fEngCsv(snap, srce, inCsvPath, outCsvPath, cols):
  # Initial Vars
  inDf = pd.read_csv(inCsvPath)

  # NEW cols
  inDf['SnapDate'] = inDf.apply(lambda row: snap, axis=1)
  inDf['UFxMt2'] = inDf.apply(lambda row: int(row['PriceUF']/row['MtTot']), axis=1)
  inDf['ValueScore'] = inDf.apply(lambda row: int(((row['Bdroom']*4)+(row['Bath']*4)+(row['Parking']*2))*(row['MtTot'])), axis=1)
  # inDf['ValueScore'] = inDf.apply(lambda row: int(((row['Bdroom']*4)+(row['Bath']*4)+(row['Parking']*2))*(row['MtTot'])*(1 if row['PropertyType'] == "Aparment" else 0.7)), axis=1)
  inDf['Score'] = inDf.apply(lambda row: int(row['ValueScore']*1000/row['PriceUF']), axis=1)

  # DROP outliers rows
  inDf.drop(inDf[inDf.Score > 4000].index, inplace=True)

  # Reorder Columns
  inDf = inDf[cols]

  # WRITE csv
  inDf.to_csv(outCsvPath, index=False)

# MAIN
def fEngMain(log, baseOutPath, statsPath, snap, inCsvPaths, cols):
  # TIME start
  startTime, startStamp = getTimeAndStamp()
  # INIT
  currStep = "ETL_04_FEATURE_ENG"
  logPrint(log, "{} Start: {}".format(currStep, str(startStamp)))

  # SET
  outSnapPath = baseOutPath + "/" + snap
  statsSnapPath = statsPath + "/" + snap
  outFillFiles = []

  ## iterate SOURCES
  for inCsvPath in inCsvPaths:
    srce = getRawFileName(inCsvPath)
    logPrint(log, "Processing Source: {}".format(inCsvPath))

    # OUTPUT setup
    outSnapStepPath = outSnapPath + "/" + currStep
    outCsvPath = initFilePath(outSnapStepPath, srce + ".csv")

    ## FORMAT
    fEngCsv(
      snap,
      srce, 
      inCsvPath, # inFile
      outCsvPath, # outFile
      cols) # column order
    outFillFiles.append(outCsvPath)
  ## FINISH
  # END TIMING & LOG
  endTime, endStamp = getTimeAndStamp()
  logPrint(log, "{} End: {}".format(currStep, str(endStamp)))
  logPrint(log, "{} Total Execution Time: {}".format(currStep, str(endTime - startTime)))
  ## STATS
  statsSnapStepPath = statsSnapPath + "/" + currStep
  stepEnd(log, statsSnapStepPath, currStep, outFillFiles)
  return outFillFiles
