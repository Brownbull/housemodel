# -*- coding: utf-8 -*-
"""
  @author: Brownbull - Gabriel Carcamo - carcamo.gabriel@gmail.com
  Clean - Clean Module inside Transform on ETL for houses model
"""
import pandas as pd
from main.include.logs import *
from main.include.files import *
from main.include.program import *
from main.ETL.stats import *

def toCsvRow(row):
  return "\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\"\n".format(
    row['Srce'], 
    row['Region'], 
    row['Province'], 
    row['PublishedDate'], 
    row['PropertyType'], 
    row['PropertyState'], 
    row['Age'], 
    row['Stage'], 
    row['Delivery'], 
    row['CommonExpnsCLP'], 
    row['Floor'], 
    row['ForInvestment'], 
    row['MtTot'], 
    row['MtUtil'], 
    row['Bdroom'], 
    row['Bath'], 
    row['Parking'], 
    row['Storage'], 
    row['Pool'], 
    row['PriceUF'], 
    row['Link'])

def clean_portalinmobiliario(inDf, outCsvPath):
  default = -16
  minPropertyArea = 15 # 15 mt2
  # dateFormat = "%Y/%m/%d"
  with open(outCsvPath, 'a', encoding="utf-8") as outCsv:  
    for idx, row in inDf.iterrows():
      writeRow = True
      # EVALUATE values
      if row['Parking'] > 10 or (row['MtUtil'] == default and row['MtTot'] == default):
        writeRow = False
      if row['MtTot'] < minPropertyArea and row['MtTot'] < minPropertyArea:
        writeRow = False

      # WRITE row
      if writeRow:
        outCsv.write(toCsvRow(row))

# CSV processing
def cleanCsv(srce, inCsvPath, outCsvPath):
  # Initial Vars
  inDf = pd.read_csv(inCsvPath)

  # SRCE switch
  if srce == "portal inmobiliario":
    clean_portalinmobiliario(inDf, outCsvPath)
  else:
    print("srce {} is not supported".format(srce))
    exit()

# MAIN
def cleanMain(log, snap, inCsvPaths, baseOutPath, statsPath, cols):
  # TIME start
  startTime, startStamp = getTimeAndStamp()
  # INIT
  currStep = "ETL_T_02_CLEAN"
  logPrint(log, "{} Start: {}".format(currStep, str(startStamp)))

  # SET
  outSnapPath = baseOutPath + "/" + snap
  statsSnapPath = statsPath + "/" + snap
  outFormatFiles = []

  ## iterate SOURCES
  for inCsvPath in inCsvPaths:
    logPrint(log, "Processing Source: {}".format(inCsvPath))

    # OUTPUT setup
    outCsvPath = initFilePath(outSnapPath, currStep + ".csv")

    # WRITE header
    with open(outCsvPath, 'w', encoding="utf-8") as outCsv:
      outCsv.write(array2Str(cols, ',') + "\n")

    ## FORMAT
    cleanCsv(
      "portal inmobiliario", 
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
