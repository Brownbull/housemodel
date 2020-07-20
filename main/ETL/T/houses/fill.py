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
  Houses Fills
  Srce = never null
  Province = never null
  PublishedDate = mode
  PropertyType = never null
  PropertyState = mode
  MtTot = median
  Bdroom = median
  Bath = median
  Parking = median
  PriceUF = median
  Link = never null
  """
  # Means
  mean_MtTot = round(inDf['MtTot'].mean(skipna=True),0)
  mean_Bdroom = round(inDf['Bdroom'].mean(skipna=True),0)
  mean_Bath = round(inDf['Bath'].mean(skipna=True),0)
  mean_Parking = round(inDf['Parking'].mean(skipna=True),0)
  mean_PriceUF = round(inDf['PriceUF'].mean(skipna=True),0)

  # Filling
  ## Blank replacement
  tempMode = inDf['PublishedDate'].mode()
  PublishedDateLastMode = "06/26/2020"
  print("on {} : {}".format( srce, PublishedDateLastMode))
  if len(tempMode) > 0:
    inDf['PublishedDate'].fillna(tempMode[0], inplace = True)
    PublishedDateLastMode = tempMode[0]
  else:
    inDf['PublishedDate'].fillna(PublishedDateLastMode, inplace = True)
  inDf['PropertyState'].fillna(inDf['PropertyState'].mode()[0], inplace = True)
  inDf['MtTot'].fillna(inDf['MtTot'].median(), inplace = True)
  inDf['Bdroom'].fillna(inDf['Bdroom'].median(), inplace = True)
  inDf['Bath'].fillna(inDf['Bath'].median(), inplace = True)
  inDf['Parking'].fillna(inDf['Parking'].median(), inplace = True)
  inDf['PriceUF'].fillna(inDf['PriceUF'].median(), inplace = True)

  ## 0 replacement
  inDf['PublishedDate'] = inDf.PublishedDate.mask(inDf.PublishedDate == 0,PublishedDateLastMode)
  # inDf['MtTot'] = inDf.MtTot.mask(inDf.MtTot == 0,mean_MtTot)
  # inDf['Bdroom'] = inDf.Bdroom.mask(inDf.Bdroom == 0,mean_Bdroom)
  # inDf['Bath'] = inDf.Bath.mask(inDf.Bath == 0,mean_Bath)
  # inDf['Parking'] = inDf.Parking.mask(inDf.Parking == 0,mean_Parking)
  # inDf['PriceUF'] = inDf.PriceUF.mask(inDf.PriceUF == 0,mean_PriceUF)

  ## Overwrite column types
  inDf['Parking'] = inDf['Parking'].astype(int)

  # WRITE csv
  inDf.to_csv(outCsvPath, index=False)

# MAIN
def fillMain(log, baseOutPath, statsPath, snap, inCsvPaths):
  # TIME start
  startTime, startStamp = getTimeAndStamp()
  # INIT
  currStep = "ETL_04_FILL"
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
