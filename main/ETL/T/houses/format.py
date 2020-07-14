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

def inferPublishedDate(string, default):
  for e in string.split():
    e = ifDateSave(e, default)
    if e != default:
      return e
  return default

def getPropertyState(string):
  return "nueva" if "nueva" in str(string).lower() else "usada"

def inferPropertyState(Description):
  Description = str(Description).lower()
  newKeys = ["nueva", "nuevo"]
  usedKeys = ["antiguo", "antigua", "usado", "usada", "remodelar", 
    "estado", "condiciones", "arriendo", "actualmente", "remodelar", 
    "remodelado", "remodelada", "mantenida", "mantenido", "rehicieron",
    "fue", "fueron"]
  if strOfListInPhrase(usedKeys, Description):
    return "usada"
  elif strOfListInPhrase(newKeys, Description):
    return "nueva"
  else:
    return "usada"

def getMtTot(string, default):
  numbrs = getNumbrs(string)
  if len(numbrs) > 0:
    Mtot = int(float(numbrs[0]))
    return Mtot if Mtot > 80 else default  
  else:
    return default

def inferMtTot(string, default):
  for e in string.split():
    e = getMtTot(e, default)
    if e != default and e < 1000:
      return e
  return default

def getBdroom(string, default):
  numbrs = getNumbrs(string)
  if len(numbrs) > 0:
    return int(float(numbrs[0]))    
  else:
    return default

def inferBdroom(string, default):
  for e in str(string).split():
    e = getBdroom(e, default)
    if e != default and e < 100:
      return e
  return default

def getBath(string, default):
  numbrs = getNumbrs(string)
  if len(numbrs) > 0:
    return int(float(numbrs[0]))    
  else:
    return default

def inferBath(string, default):
  for e in str(string).split():
    e = getBath(e, default)
    if e != default and e < 100:
      return e
  return default

def getParking(string, default):
  numbrs = getNumbrs(string)
  if len(numbrs) > 0:
    return int(float(numbrs[0]))    
  else:
    return default

def inferParking(Description):
  Description = str(Description).lower()
  noParkingKeys = ["no", "tiene"]
  if "estacionamientos" in Description:
    return 2
  elif "estacionamiento" in Description and not strListInPhrase(noParkingKeys, Description):
    return 1
  else:
    return 0

def getPriceUF(string, default):
  UF = 28800 # 07/13/2020 = $28.684 CLP
  floatCorrectionUF = 1000
  floatCorrectionCLP = 1000000
  numbrs = getNumbrs(string)
  if len(numbrs) > 0:
    if '$' in str(string):
      return int((float(numbrs[0])*floatCorrectionCLP)/UF)
    else:    
      return int(float(numbrs[0])*floatCorrectionUF)
  else:
    return default

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
      PropertyState = getPropertyState(row['PropertyState'])
      MtTot = getMtTot(row['MtTot'], default)      
      Bdroom = getBdroom(row['Bdroom'], default)
      Bath = getBath(row['Bath'], default)
      Parking = getParking(row['Parking'], default)
      PriceUF = getPriceUF(row['PriceUF'], default)
      Link = row['Link']

      # WRITE row
      houseToWrite = TrasformHouse( Srce, Province, PublishedDate, PropertyType, PropertyState, MtTot, Bdroom, Bath, Parking,  PriceUF, Link)
      outCsv.write(houseToWrite.toCsvRow())

def format_toctoc(inDf, outCsvPath):
  default = "TBD"
  with open(outCsvPath, 'a', encoding="utf-8") as outCsv:  
    for idx, row in inDf.iterrows():
      # GET values
      Srce = row['Srce']
      Province = row['Province']
      PublishedDate = ifDateSave(row['PublishedDate'], default)
      PropertyType = row['PropertyType']
      PropertyState = inferPropertyState(row['Description'])
      MtTot = getMtTot(row['MtTot'], default)
      Bdroom = getBdroom(row['Bdroom'], default)
      Bath = getBath(row['Bath'], default)
      Parking = inferParking(row['Description'])
      PriceUF = getPriceUF(row['PriceUF'], default)
      Link = row['Link']

      # WRITE row
      houseToWrite = TrasformHouse( Srce, Province, PublishedDate, PropertyType, PropertyState, MtTot, Bdroom, Bath, Parking,  PriceUF, Link)
      outCsv.write(houseToWrite.toCsvRow())

def format_propiedadesemol(inDf, outCsvPath):
  default = "TBD"
  with open(outCsvPath, 'a', encoding="utf-8") as outCsv:  
    for idx, row in inDf.iterrows():
      # GET values
      Srce = row['Srce']
      Province = row['Province']
      PublishedDate = inferPublishedDate(row['PublishedDate'], default)
      PropertyType = row['PropertyType']
      PropertyState = inferPropertyState(row['Description'])
      MtTot = inferMtTot(row['MtTot'], default)
      Bdroom = inferBdroom(row['Bdroom'], default)
      Bath = inferBath(row['Bath'], default)
      Parking = inferParking(row['Description'])
      PriceUF = getPriceUF(row['PriceUF'], default)
      Link = row['Link']

      # WRITE row
      houseToWrite = TrasformHouse( Srce, Province, PublishedDate, PropertyType, PropertyState, MtTot, Bdroom, Bath, Parking,  PriceUF, Link)
      outCsv.write(houseToWrite.toCsvRow())

def format_icasas(inDf, outCsvPath):
  default = "TBD"
  with open(outCsvPath, 'a', encoding="utf-8") as outCsv:  
    for idx, row in inDf.iterrows():
      # GET values
      Srce = row['Srce']
      Province = row['Province']
      PublishedDate = row['PublishedDate']
      PropertyType = row['PropertyType']
      PropertyState = inferPropertyState(row['Description'])
      MtTot = getMtTot(row['MtTot'], default)
      Bdroom = inferBdroom(row['Bdroom'], default)
      Bath = inferBath(row['Bath'], default)
      Parking = inferParking(row['Description'])
      PriceUF = getPriceUF(row['PriceUF'], default)
      Link = row['Link']

      # WRITE row
      houseToWrite = TrasformHouse( Srce, Province, PublishedDate, PropertyType, PropertyState, MtTot, Bdroom, Bath, Parking,  PriceUF, Link)
      outCsv.write(houseToWrite.toCsvRow())

def format_chilepropiedades(inDf, outCsvPath):
  default = "TBD"
  with open(outCsvPath, 'a', encoding="utf-8") as outCsv:  
    for idx, row in inDf.iterrows():
      # GET values
      Srce = row['Srce']
      Province = row['Province']
      PublishedDate = row['PublishedDate']
      PropertyType = row['PropertyType']
      PropertyState = inferPropertyState(row['Description'])
      MtTot = getMtTot(row['MtTot'], default)
      Bdroom = row['Bdroom']
      Bath = row['Bath']
      Parking = inferParking(row['Description'])
      PriceUF = getPriceUF(row['PriceUF'], default)
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
