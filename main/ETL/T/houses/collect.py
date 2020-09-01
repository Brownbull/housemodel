# -*- coding: utf-8 -*-
"""
  @author: Brownbull - Gabriel Carcamo - carcamo.gabriel@gmail.com
  Format - Format Module inside Transform on ETL for houses model
"""
import pandas as pd
from main.include.logs import *
from main.include.files import *
from main.include.program import *
from main.ETL.stats import *

def toCsvRow(Srce, Region, PropertyType, PropertyState, PublishedDate, Title, Section, Models, fld0, fld1, fld2, fld3, fld4, fld5, FullPrice, Link, Description):
  return "\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\"\n".format(
    Srce ,
    Region ,
    PropertyType ,
    PropertyState, 
    PublishedDate, 
    Title, 
    Section, 
    Models, 
    fld0, 
    fld1, 
    fld2, 
    fld3, 
    fld4, 
    fld5, 
    FullPrice, 
    Link, 
    Description)

def collect_portalinmobiliario(srceRow, inDf, outCsvPath):
  default = ""
  with open(outCsvPath, 'a', encoding="utf-8") as outCsv:  
    # FROM SRCE
    Srce = sanitize(srceRow['Srce'])
    Region = sanitize(srceRow['Region'])
    PropertyType = sanitize(srceRow['PropertyType'])

    # FROM CSV
    for idx, row in inDf.iterrows():
      PropertyState = sanitize(row['PropertyState'])
      PublishedDate = sanitize(row['PublishedDate'])
      Title = sanitize(row['Title'])
      Section = sanitize(row['Section'])
      Models = sanitize(row['Models'])
      fld0 = sanitize(row['fld0'])
      fld1 = sanitize(row['fld1'])
      fld2 = sanitize(row['fld2'])
      fld3 = sanitize(row['fld3'])
      fld4 = sanitize(row['fld4'])
      fld5 = sanitize(row['fld5'])
      FullPrice = sanitize(row['item'])
      Link = sanitize(row['item-href'])
      Description = sanitize(row['Description'])

      # WRITE row
      if not isNaN(Link) and not isNaN(fld0) and not isNaN(Description):
        rowToWrite = toCsvRow(Srce, Region, PropertyType, PropertyState, PublishedDate, Title, Section, Models, fld0, fld1, fld2, fld3, fld4, fld5, FullPrice, Link, Description)
        outCsv.write(rowToWrite)

def collectCsv(outFile, srce, idxRow, inCsvPath):
  # DF init
  inDf = pd.read_csv(inCsvPath)

  # SRCE switch
  if srce == "portal inmobiliario":
    collect_portalinmobiliario(idxRow, inDf, outFile)
  else:
    print("srce {} is not supported".format(srce))
    exit()


# MAIN
def collectMain(log, snap, dataIndex, baseInPath, baseOutPath, statsPath, srces, cols):
  # TIME start
  startTime, startStamp = getTimeAndStamp()
  # INIT
  currStep = "ETL_T_00_COLLECT"
  logPrint(log, "{} Start: {}".format(currStep, str(startStamp)))

  # SET
  inSnapPath = baseInPath + "/" + snap 
  outSnapPath = baseOutPath + "/" + snap
  statsSnapPath = statsPath + "/" + snap
  outCollectFiles = []

  ## iterate SOURCES
  for srce in srces:
    logPrint(log, "Processing Source: {}".format(srce))

    # OUTPUT setup
    outSnapStepPath = outSnapPath + "/" + currStep
    outSrcPath = initFilePath(outSnapStepPath, srce + ".csv")

    # WRITE header
    with open(outSrcPath, 'w', encoding="utf-8") as outCsv:
      outCsv.write(array2Str(cols, ',') + "\n")

    # INPUT search
    inSnapSrcePath = inSnapPath + "/" + srce 
    inCsvFiles = os.listdir(inSnapSrcePath)

    ## iterate CSV
    for inCsv in inCsvFiles:
      # INPUT identification
      inCsvPath = inSnapSrcePath + "/" + inCsv
      csvId = inCsv.split('.')[0]
      idxRow = dataIndex.iloc[int(csvId)-1]

      ## COLLECT
      collectCsv(
        outSrcPath, # outFile
        srce, 
        idxRow, 
        inCsvPath, # csvPath
        )

    # SAVE output
    outCollectFiles.append(outSrcPath)

  ## FINISH
  logPrint(log, "{} Output Files: {}".format(currStep, array2Str(outCollectFiles, '\n\t')))
  # END TIMING & LOG
  endTime, endStamp = getTimeAndStamp()
  logPrint(log, "{} End: {}".format(currStep, str(endStamp)))
  logPrint(log, "{} Total Execution Time: {}".format(currStep, str(endTime - startTime)))
  ## STATS
  statsSnapStepPath = statsSnapPath + "/" + currStep
  stepEnd(log, statsSnapStepPath, currStep, outCollectFiles)
  return outCollectFiles
