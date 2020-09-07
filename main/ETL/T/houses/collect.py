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

def collect_portalinmobiliario(inDf, outCsvPath):
  default = ""
  with open(outCsvPath, 'a', encoding="utf-8") as outCsv:  
    # FROM SRCE
    Srce = sanitize("portal inmobiliario")
    Region = sanitize("rm")
    PropertyType = sanitize("-16")

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
      Link = sanitize(row['link'])
      Description = sanitize(row['Description'])

      # WRITE row
      if not isNaN(Link) and not isNaN(fld0) and not isNaN(Description):
        rowToWrite = toCsvRow(Srce, Region, PropertyType, PropertyState, PublishedDate, Title, Section, Models, fld0, fld1, fld2, fld3, fld4, fld5, FullPrice, Link, Description)
        outCsv.write(rowToWrite)

def collectCsv(outFile, srce, inCsvPath):
  # DF init
  inDf = pd.read_csv(inCsvPath)

  # SRCE switch
  if srce == "portal inmobiliario":
    collect_portalinmobiliario(inDf, outFile)
  else:
    print("srce {} is not supported".format(srce))
    exit()


# MAIN
def collectMain(log, snap, inCsvPath, baseOutPath, statsPath, srces, cols):
  # TIME start
  startTime, startStamp = getTimeAndStamp()
  # INIT
  currStep = "ETL_T_00_COLLECT"
  logPrint(log, "{} Start: {}".format(currStep, str(startStamp)))

  # SET
  outSnapPath = baseOutPath + "/" + snap
  statsSnapPath = statsPath + "/" + snap
  outCollectFiles = []

  # OUTPUT setup
  outSrcPath = initFilePath(outSnapPath, currStep + ".csv")

  # WRITE header
  with open(outSrcPath, 'w', encoding="utf-8") as outCsv:
    outCsv.write(array2Str(cols, ',') + "\n")

  ## COLLECT
  collectCsv(
    outSrcPath, # outFile
    "portal inmobiliario", 
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
