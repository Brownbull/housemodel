# -*- coding: utf-8 -*-
"""
  @author: Brownbull - Gabriel Carcamo - carcamo.gabriel@gmail.com
  Fill Module inside Transform on ETL for houses model
"""
import pandas as pd
from include.logs import *
from include.files import *
from include.program import *
from main.ETL.stats import *

global PublishedDateLastMode

# CSV processing
def fillCsv(srce, inCsvPath, outCsvPath):
  # Initial Vars
  inDf = pd.read_csv(inCsvPath)
  """
  Srce : never null
  Region : never null
  Province : never null
  PublishedDate : never null
  PropertyType : never null
  PropertyState : never null
  Age : -16
  Stage : -16
  Delivery : -16
  CommonExpnsCLP : -16 
  Floor : -16
  ForInvestment : N 
  MtTot : fEng
  MtUtil : fEng
  Bdroom : never null
  Bath : never null
  Parking : never null
  Storage : never null
  Pool : 0
  PriceUF : never null 
  Link : never null
  """
  ## Means
  # mean_MtTot = round(inDf['MtTot'].mean(skipna=True),0)
  ## Filling
  # inDf['column'] = inDf['column'].replace(np.nan, 0)
  # inDf['PropertyState'].fillna(inDf['PropertyState'].mode()[0], inplace = True)
  ## 0 replacement
  # inDf['MtTot'] = inDf.MtTot.mask(inDf.MtTot == 0,mean_MtTot)
  ## Overwrite column types
  # inDf['Parking'] = inDf['Parking'].astype(int)

  # WRITE csv
  inDf.to_csv(outCsvPath, index=False)

# MAIN
def fillMain(log, snap, inCsvPaths, baseOutPath, statsPath):
  # TIME start
  startTime, startStamp = getTimeAndStamp()
  # INIT
  currStep = "ETL_T_04_FILL"
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
    fillCsv(
      srce, 
      inCsvPath, # inFile
      outCsvPath) # outFile

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
