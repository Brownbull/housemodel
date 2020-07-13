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

class CollectHouse:
  def __init__(self, Srce, Province, PublishedDate, PropertyType, PropertyState, MtTot, Bdroom, Bath, Parking,  PriceUF, Link, Description):
    self.Srce = Srce
    self.Province = Province
    self.PublishedDate = PublishedDate
    self.PropertyType = PropertyType
    self.PropertyState = PropertyState
    self.MtTot = MtTot
    self.Bdroom = Bdroom
    self.Bath = Bath
    self.Parking = Parking
    self.PriceUF = PriceUF
    self.Link = Link
    self.Description = Description

  def toCsvRow(self):
    return "\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\"\n".format(
      self.Srce ,
      self.Province ,
      self.PublishedDate ,
      self.PropertyType ,
      self.PropertyState ,
      self.MtTot ,
      self.Bdroom ,
      self.Bath ,
      self.Parking ,
      self.PriceUF,
      self.Link,
      self.Description)

def collect_portalinmobiliario(srce, province, propertyType, inDf, outCsvPath):
  with open(outCsvPath, 'a', encoding="utf-8") as outCsv:  
    for idx, row in inDf.iterrows():
      PublishedDate = row['PublishedDate']
      PropertyState = row['PropertyState']
      MtTot = row['MtTot']
      Bdroom = row['Bdroom']
      Bath = row['Bath']
      Parking = row['Parking']
      PriceUF = "{} {}".format(row['Price'], row['PriceUnit'])
      Link = row['item-href']
      Description = rmEscSep(row['Description']).replace(",","")

      # WRITE row
      houseToWrite = CollectHouse( srce, province, PublishedDate, propertyType, PropertyState, MtTot, Bdroom, Bath, Parking,  PriceUF, Link, Description)
      outCsv.write(houseToWrite.toCsvRow())

def collect_toctoc(srce, province, propertyType, inDf, outCsvPath):
  with open(outCsvPath, 'a', encoding="utf-8") as outCsv:  
    for idx, row in inDf.iterrows():
      PublishedDate = row['PublishedDate']
      PropertyState = "TBD"
      MtTot = row['MtTot']
      Bdroom = row['Bdroom']
      Bath = row['Bath']
      Parking = "TBD"
      PriceUF = row['FullPrice']
      Link = row['item-href']
      Description = rmEscSep(row['Description']).replace(",","")

      # WRITE row
      houseToWrite = CollectHouse( srce, province, PublishedDate, propertyType, PropertyState, MtTot, Bdroom, Bath, Parking,  PriceUF, Link, Description)
      outCsv.write(houseToWrite.toCsvRow())

def collect_propiedadesemol(srce, province, propertyType, inDf, outCsvPath):
  with open(outCsvPath, 'a', encoding="utf-8") as outCsv:  
    for idx, row in inDf.iterrows():
      PublishedDate = row['PublishedDate']
      PropertyState = "TBD"
      MtTot = row['MtTot']
      Bdroom = row['Bdroom']
      Bath = row['Bath']
      Parking = "TBD"
      PriceUF = row['FullPrice']
      Link = row['item-href']
      Description = rmEscSep(row['Description']).replace(",","")

      # WRITE row
      houseToWrite = CollectHouse( srce, province, PublishedDate, propertyType, PropertyState, MtTot, Bdroom, Bath, Parking,  PriceUF, Link, Description)
      outCsv.write(houseToWrite.toCsvRow())

def collect_icasas(srce, province, propertyType, inDf, outCsvPath):
  with open(outCsvPath, 'a', encoding="utf-8") as outCsv:  
    for idx, row in inDf.iterrows():
      PublishedDate = "TBD"
      PropertyState = "TBD"
      MtTot = row['MtTot']
      Bdroom = row['Bdroom']
      Bath = row['Bath']
      Parking = "TBD"
      PriceUF = row['FullPrice']
      Link = row['item-href']
      Description = rmEscSep(row['Description']).replace(",","")

      # WRITE row
      houseToWrite = CollectHouse( srce, province, PublishedDate, propertyType, PropertyState, MtTot, Bdroom, Bath, Parking,  PriceUF, Link, Description)
      outCsv.write(houseToWrite.toCsvRow())
  
def collect_chilepropiedades(srce, province, propertyType, inDf, outCsvPath):
  with open(outCsvPath, 'a', encoding="utf-8") as outCsv:  
    for idx, row in inDf.iterrows():
      PublishedDate = "TBD"
      PropertyState = "TBD"
      MtTot = row['MtTot']
      Bdroom = row['Bdroom']
      Bath = row['Bath']
      Parking = "TBD"
      PriceUF = row['FullPrice']
      Link = row['item-href']
      Description = rmEscSep(row['Description']).replace(",","")
      
      # WRITE row
      houseToWrite = CollectHouse( srce, province, PublishedDate, propertyType, PropertyState, MtTot, Bdroom, Bath, Parking,  PriceUF, Link, Description)
      outCsv.write(houseToWrite.toCsvRow())

# CSV processing
def collectCsv(outFile, srce, idxRow, inCsvPath):
  # Initial Vars
  inDf = pd.read_csv(inCsvPath)
  province = idxRow['Province']
  propertyType = idxRow['PropertyType']

  # SRCE switch
  if srce == "portal inmobiliario":
    collect_portalinmobiliario(srce, province, propertyType, inDf, outFile)
  elif srce == "toctoc":
    collect_toctoc(srce, province, propertyType, inDf, outFile)
  elif srce == "propiedades emol":
    collect_propiedadesemol(srce, province, propertyType, inDf, outFile)
  elif srce == "icasas":
    collect_icasas(srce, province, propertyType, inDf, outFile)
  elif srce == "chile propiedades":
    collect_chilepropiedades(srce, province, propertyType, inDf, outFile)

# MAIN
def collectMain(log, dataIndex, baseInPath, baseOutPath, statsPath, snap, srces, collectionCols):
  # TIME start
  startTime, startStamp = getTimeAndStamp()
  # INIT
  currStep = "ETL_01_COLLECT"
  logPrint(log, "{} Start: {}".format(currStep, str(startStamp)))
  # logPrint(log, "ETL Step Start: {}".format(currStep))

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
      outCsv.write(array2Str(collectionCols, ',') + "\n")

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
        inCsvPath) # csvPath

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