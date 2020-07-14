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

class TrasformHouse:
  def __init__(self, Srce, Province, PublishedDate, PropertyType, PropertyState, MtTot, Bdroom, Bath, Parking,  PriceUF, Link):
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

  def toCsvRow(self):
    return "\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\"\n".format(
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
      self.Link)

def format_portalinmobiliario(inDf, outCsvPath):
  default = "TBD"
  # dateFormat = "%Y/%m/%d"
  with open(outCsvPath, 'a', encoding="utf-8") as outCsv:  
    for idx, row in inDf.iterrows():
      # GET values
      Srce = row['Srce']
      Province = row['Province']
      PublishedDate = ifDateSave(row['PublishedDate'], default)

      PropertyType = row['PropertyType']
      PropertyState = row['PropertyState']
      MtTot = row['MtTot']
      Bdroom = row['Bdroom']
      Bath = row['Bath']
      Parking = row['Parking']
      PriceUF = row['PriceUF']
      Link = row['Link']

      # WRITE row
      houseToWrite = TrasformHouse( Srce, Province, PublishedDate, PropertyType, PropertyState, MtTot, Bdroom, Bath, Parking,  PriceUF, Link)
      outCsv.write(houseToWrite.toCsvRow())

def format_toctoc(inDf, outCsvPath):
  with open(outCsvPath, 'a', encoding="utf-8") as outCsv:  
    for idx, row in inDf.iterrows():
      # GET values
      Srce = row['Srce']
      Province = row['Province']
      PublishedDate = row['PublishedDate']
      PropertyType = row['PropertyType']
      PropertyState = row['PropertyState']
      MtTot = row['MtTot']
      Bdroom = row['Bdroom']
      Bath = row['Bath']
      Parking = row['Parking']
      PriceUF = row['PriceUF']
      Link = row['Link']

      # WRITE row
      houseToWrite = TrasformHouse( Srce, Province, PublishedDate, PropertyType, PropertyState, MtTot, Bdroom, Bath, Parking,  PriceUF, Link)
      outCsv.write(houseToWrite.toCsvRow())

def format_propiedadesemol(inDf, outCsvPath):
  with open(outCsvPath, 'a', encoding="utf-8") as outCsv:  
    for idx, row in inDf.iterrows():
      # GET values
      Srce = row['Srce']
      Province = row['Province']
      PublishedDate = row['PublishedDate']
      PropertyType = row['PropertyType']
      PropertyState = row['PropertyState']
      MtTot = row['MtTot']
      Bdroom = row['Bdroom']
      Bath = row['Bath']
      Parking = row['Parking']
      PriceUF = row['PriceUF']
      Link = row['Link']

      # WRITE row
      houseToWrite = TrasformHouse( Srce, Province, PublishedDate, PropertyType, PropertyState, MtTot, Bdroom, Bath, Parking,  PriceUF, Link)
      outCsv.write(houseToWrite.toCsvRow())

def format_icasas(inDf, outCsvPath):
  with open(outCsvPath, 'a', encoding="utf-8") as outCsv:  
    for idx, row in inDf.iterrows():
      # GET values
      Srce = row['Srce']
      Province = row['Province']
      PublishedDate = row['PublishedDate']
      PropertyType = row['PropertyType']
      PropertyState = row['PropertyState']
      MtTot = row['MtTot']
      Bdroom = row['Bdroom']
      Bath = row['Bath']
      Parking = row['Parking']
      PriceUF = row['PriceUF']
      Link = row['Link']

      # WRITE row
      houseToWrite = TrasformHouse( Srce, Province, PublishedDate, PropertyType, PropertyState, MtTot, Bdroom, Bath, Parking,  PriceUF, Link)
      outCsv.write(houseToWrite.toCsvRow())

def format_chilepropiedades(inDf, outCsvPath):
  with open(outCsvPath, 'a', encoding="utf-8") as outCsv:  
    for idx, row in inDf.iterrows():
      # GET values
      Srce = row['Srce']
      Province = row['Province']
      PublishedDate = row['PublishedDate']
      PropertyType = row['PropertyType']
      PropertyState = row['PropertyState']
      MtTot = row['MtTot']
      Bdroom = row['Bdroom']
      Bath = row['Bath']
      Parking = row['Parking']
      PriceUF = row['PriceUF']
      Link = row['Link']

      # WRITE row
      houseToWrite = TrasformHouse( Srce, Province, PublishedDate, PropertyType, PropertyState, MtTot, Bdroom, Bath, Parking,  PriceUF, Link)
      outCsv.write(houseToWrite.toCsvRow())


# CSV processing
def formatCsv(srce, inCsvPath, outCsvPath):
  # Initial Vars
  inDf = pd.read_csv(inCsvPath)

  # SRCE switch
  if srce == "portal inmobiliario":
    houseToWrite = format_portalinmobiliario(inDf, outCsvPath)
  elif srce == "toctoc":
    houseToWrite = format_toctoc(inDf, outCsvPath)
  elif srce == "propiedades emol":
    houseToWrite = format_propiedadesemol(inDf, outCsvPath)
  elif srce == "icasas":
    houseToWrite = format_icasas(inDf, outCsvPath)
  elif srce == "chile propiedades":
    houseToWrite = format_chilepropiedades(inDf, outCsvPath)

# MAIN
def formatMain(log, baseOutPath, statsPath, snap, inCsvPaths, formatCols):
  # TIME start
  startTime, startStamp = getTimeAndStamp()
  # INIT
  currStep = "ETL_02_FORMAT"
  logPrint(log, "asd {} Start: {}".format(currStep, str(startStamp)))

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
    outCsv = formatCsv(
      srce, 
      inCsvPath, # inFile
      outCsvPath) # outFile

    outFormatFiles.append(outCsvPath)
  exit()
  ## FINISH
  # END TIMING & LOG
  endTime, endStamp = getTimeAndStamp()
  logPrint(log, "{} End: {}".format(currStep, str(endStamp)))
  logPrint(log, "{} Total Execution Time: {}".format(currStep, str(endTime - startTime)))
  ## STATS
  statsSnapStepPath = statsSnapPath + "/" + currStep
  stepEnd(log, statsSnapStepPath, currStep, outFormatFiles)
  return outFormatFiles
