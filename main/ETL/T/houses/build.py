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

def getProvince(rawProvince, rawProvincePref2, rawProvincePref3, default=-16):
  if rawProvince == "santiago":
    return "santiago"
  elif rawProvince == "huechuraba":
    return "huechuraba"
  elif rawProvince == "nunoa":
    return "nunoa"
  elif rawProvince == "providencia":
    return "providencia"
  elif rawProvince == "condes":
    return "las condes"
  elif rawProvince == "vitacura":
    return "vitacura"
  elif rawProvince == "barnechea":
    return "lo barnechea"
  elif rawProvince == "florida":
    return "la florida"
  elif rawProvince == "maipu":
    return "maipu"
  elif rawProvince == "penalolen":
    return "penalolen"
  elif rawProvince == "alto":
    return "puente alto"
  elif rawProvince == "central":
    return "estacion central"
  elif rawProvince == "normal":
    return "quinta normal"
  elif rawProvince == "prado":
    return "lo prado"
  elif rawProvince == "conchali":
    return "lo conchali"
  elif rawProvince == "quilicura":
    return "quilicura"
  elif rawProvince == "macul":
    return "macul"
  elif rawProvince == "reina":
    return "reina"
  elif rawProvince == "penaflor":
    return "penaflor"
  elif rawProvince == "colina":
    return "colina"
  elif rawProvince == "pirque":
    return "pirque"
  elif rawProvince == "independencia":
    return "independencia"
  elif rawProvince == "pudahuel":
    return "pudahuel"
  elif rawProvince == "recoleta":
    return "recoleta"
  elif rawProvince == "renca":
    return "renca"
  elif rawProvince == "lampa":
    return "lampa"
  elif rawProvince == "paine":
    return "paine"
  elif rawProvince == "tiltil":
    return "tiltil"
  elif rawProvince == "buin":
    return "buin"
  elif rawProvince == "melipilla":
    return "melipilla"
  elif rawProvince == "monte":
    return "el monte"
  elif rawProvince == "curacavi":
    return "curacavi"
  elif rawProvince == "cerrillos":
    return "cerrillos"
  elif rawProvince == "talagante":
    return "talagante"
  elif rawProvince == "granja":
    return "la granja"
  elif rawProvince == "pinto":
    return "maria pinto"
  elif rawProvince == "hurtado":
    return "padre hurtado"
  elif rawProvince == "cisterna":
    return "la cisterna"
  elif rawProvince == "miguel":
    return "san miguel"
  elif rawProvince == "bernardo":
    return "san bernardo"
  elif rawProvince == "bosque":
    return "el bosque"
  elif rawProvince == "pintana":
    return "la pintana"
  elif rawProvince == "joaquin":
    return "san joaquin"
  elif rawProvince == "ramon":
    return "san ramon"
  elif rawProvince == "navia":
    return "cerro navia"
  elif rawProvince == "cerda":
    return "pedro aguirre cerda"
  elif rawProvince == "tango":
    return "calera de tango"
  elif rawProvince == "maipo":
    if rawProvincePref3 == "jose":
      return "san jose de maipo"
    else:
      return "isla de maipo"
  else:
    return default

class buildHouse:
  def __init__(self, default=-16):
    self.Srce = default 
    self.Region = default 
    self.Province = default 
    self.PublishedDate = default 
    self.PropertyType = default 
    self.PropertyState = "usada" 
    self.Age = default 
    self.Stage = default 
    self.Delivery = default 
    self.CommonExpnsCLP = default 
    self.Floor = default 
    self.ForInvestment = 'N' 
    self.MtTot = default 
    self.MtUtil = default 
    self.Bdroom = default 
    self.Bath = default 
    self.Parking = 0 
    self.Storage = 0 
    self.Pool = 0 
    self.PriceUF = default 
    self.Link = default 

  def toCsvRow(self):
    return "\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\",\"{}\"\n".format(
      self.Srce, 
      self.Region, 
      self.Province, 
      self.PublishedDate, 
      self.PropertyType, 
      self.PropertyState, 
      self.Age, 
      self.Stage, 
      self.Delivery, 
      self.CommonExpnsCLP, 
      self.Floor, 
      self.ForInvestment, 
      self.MtTot, 
      self.MtUtil, 
      self.Bdroom, 
      self.Bath, 
      self.Parking, 
      self.Storage, 
      self.Pool, 
      self.PriceUF, 
      self.Link)

  def linkSrce(self, Srce, default=-16):
    self.Srce = Srce

  def linkRegion(self, Region, default=-16):
    self.Region = Region
    
  def linkPropertyType(self, PropertyType, default=-16):
    self.PropertyType = PropertyType

  def linkPropertyState(self, PropertyState, default=-16):
    # Flag based on Province flagging, better accuracy
    return

  def linkPublishedDate(self, PublishedDate, default=-16):
    self.PublishedDate = ifDateSave(PublishedDate, default)

  def linkTitle(self, Title, default=-16):
    words = Title.split()
    rawProvince = words[len(words)-1]
    rawProvincePref2 = words[len(words)-2]
    rawProvincePref3 = words[len(words)-3]

    self.Province = getProvince(rawProvince, rawProvincePref2, rawProvincePref3)
    
    # New properties returns default after getProvince
    if self.Province == default:
      self.PropertyState = "nueva"
    else:
      self.Stage = "Done"
    
  def linkSection(self, Section, default=-16):
    return

  """
  58.17 m totales 2 dormitorios 2 banos
  235.22 m totales 1 dormitorios 2 banos
  """
  def linkModels(self, Models, default=-16):
    if not isNaN(Models):
      # Get numeric values
      numbrs = getNumbrs(Models)
      if len(numbrs) > 0:
        # save first numeric value
        firstNmbr = int(float(numbrs[0]))
        secndNmbr = int(float(numbrs[1]))
        thirdNmbr = int(float(numbrs[2]))
        if self.MtTot == default:
          self.MtTot = firstNmbr
        if self.Bdroom == default:
          self.Bdroom = secndNmbr
        if self.Bath == default:
          self.Bath = thirdNmbr
  
  """
  dormitorios 3
  m utiles: 148.71 - 227.9
  superficie total 100.35 m
  superficie total 105 m
  superficie util 75 m
  """
  def linkfld0(self, fld0, default=-16):
    if not isNaN(fld0):
      # Get numeric values
      numbrs = getNumbrs(fld0)
      if len(numbrs) > 0:
        # save first numeric value
        firstNmbr = int(float(numbrs[0]))
        if "dormitorios" in fld0 and self.Bdroom == default:
          self.Bdroom = firstNmbr
        elif "superficie" in fld0:
          if "total" in fld0 and self.MtTot == default:
            self.MtTot = firstNmbr
          elif "util" in fld0 and self.MtUtil == default:
            self.MtUtil = firstNmbr
        elif "utiles" in fld0 and self.MtUtil == default:
          self.MtUtil = firstNmbr
          
  """
  dormitorios 2
  dormitorios: 1 - 3
  banos 2
  superficie util 100.3 m
  superficie util 100 m
  """
  def linkfld1(self, fld1, default=-16):
    if not isNaN(fld1):
      # Get numeric values
      numbrs = getNumbrs(fld1)
      if len(numbrs) > 0:
        # save first numeric value
        firstNmbr = int(float(numbrs[0]))
        if "dormitorios" in fld1 and self.Bdroom == default:
          self.Bdroom = firstNmbr
        elif "superficie" in fld1 and self.MtUtil == default:
          self.MtUtil = firstNmbr
        elif "banos" in fld1 and self.Bath == default:
          self.Bath = firstNmbr
  
  """
  ambientes 2
  dormitorios 2
  banos 1
  banos: 2
  banos: 2 - 4
  """
  def linkfld2(self, fld2, default=-16):
    if not isNaN(fld2):
      # Get numeric values
      numbrs = getNumbrs(fld2)
      if len(numbrs) > 0:
        # save first numeric value
        firstNmbr = int(float(numbrs[0]))
        if strOfListInPhrase(["dormitorios", "ambientes"], fld2) and self.Bdroom == default:
          self.Bdroom = firstNmbr
        elif "banos" in fld2 and self.Bath == default:
          self.Bath = firstNmbr
  
  """
  ambientes 1
  banos 1
  antiguedad 15 anos
  estacionamientos 1
  gastos comunes 130000 clp
  estado del proyecto: entrega inmediata
  estado del proyecto: lanzamiento
  estado del proyecto: venta en verde
  """
  def linkfld3(self, fld3, default=-16):
    if not isNaN(fld3):
      # Get numeric values
      numbrs = getNumbrs(fld3)
      if len(numbrs) > 0:
        # save first numeric value
        firstNmbr = int(float(numbrs[0]))
        if "ambientes" in fld3 and self.Bdroom == default:
          self.Bdroom = firstNmbr
        elif "banos" in fld3 and self.Bath == default:
          self.Bath = firstNmbr
        elif "antiguedad" in fld3 and self.Age == default:
          self.Age = firstNmbr
          self.Stage = "Done"
        elif "estacionamientos" in fld3 and self.Parking == 0:
          self.Parking = firstNmbr
        elif strListInPhrase(["gastos","comunes"], fld3) and self.CommonExpnsCLP == default:
          self.CommonExpnsCLP = firstNmbr
      elif "proyecto" in fld3 and self.Stage == default:
        if strListInPhrase(["entrega","inmediata"], fld3):
          self.Stage = "entrega inmediata"
        elif "lanzamiento" in fld3:
          self.Stage = "lanzamiento"
        elif strListInPhrase(["venta","verde"], fld3):
          self.Stage = "venta en verde"

  """
  banos 1
  antiguedad 0 anos
  antiguedad 2008 anos
  estacionamientos 1
  gastos comunes 120000 clp
  fecha de entrega: 2 semestre 2021
  # orientacion norte
  """ 
  def linkfld4(self, fld4, default=-16):
    if not isNaN(fld4):
      # Get numeric values
      numbrs = getNumbrs(fld4)
      if len(numbrs) > 0:
        # save first numeric value
        firstNmbr = int(float(numbrs[0]))
        if "banos" in fld4 and self.Bath == default:
          self.Bath = firstNmbr
        elif "antiguedad" in fld4 and self.Age == default:
          if firstNmbr > 200:
            self.Age = 2020 - firstNmbr
          else:
            self.Age = firstNmbr
          self.Stage = "Done"
        elif "estacionamientos" in fld4 and self.Parking == 0:
          self.Parking = firstNmbr
        elif strListInPhrase(["gastos","comunes"], fld4) and self.CommonExpnsCLP == default:
          self.CommonExpnsCLP = firstNmbr
        elif strListInPhrase(["fecha","entrega"], fld4) and self.Delivery == default:
          self.Delivery = array2Str(fld4.split()[3:], ' ')

  """
  bodegas 2
  # cantidad de pisos 1
  # departamentos por piso 2
  numero de piso de la unidad 11
  """ 
  def linkfld5(self, fld5, default=-16):
    if not isNaN(fld5):
      # Get numeric values
      numbrs = getNumbrs(fld5)
      if len(numbrs) > 0:
        # save first numeric value
        firstNmbr = int(float(numbrs[0]))
        if "bodegas" in fld5 and self.Storage == 0:
          self.Storage = firstNmbr
        elif strListInPhrase(["numero","piso","unidad"], fld5) and self.Floor == default:
          self.Floor = firstNmbr

  """
  $ 140.000.000
  uf 5.890
  """
  def linkFullPrice(self, FullPrice, default=-16):
    UF = 28800 # 07/13/2020 = $28.684 CLP
    floatCorrectionUF = 1000
    floatCorrectionCLP = 1000000
    numbrs = getNumbrs(FullPrice)
    if len(numbrs) > 0:
      if '$' in str(FullPrice):
        self.PriceUF =  int((float(numbrs[0])*floatCorrectionCLP)/UF)
      else:    
        self.PriceUF =  int(float(numbrs[0])*floatCorrectionUF)
    else:
      self.PriceUF =  default
  
  """
  https://www.portalinmobiliario.com/venta/departamento/nunoa-metropolitana/9066-edificio-new-nva#position=5&type=item&tracking_id=700b3753-3c55-4408-ad96-5748ac0fa507
  """
  def linkLink(self, Link, default=-16):
    self.Link = Link
    if self.Province == default:
      words = Link.split('/')
      rawPlace = words[5].split('-')
      rawProvince = rawPlace[len(rawPlace)-2]
      self.Province = getProvince(rawProvince, default, default)
    
  def linkDescription(self, Description, default=-16):
    investKeys = ["remodelar", "inversion"]
    if strOfListInPhrase(investKeys, Description):
      self.ForInvestment = 'Y'
    if self.Parking == 0 or self.Parking > 10:
      noParkingKeys = ["no", "tiene"]
      if "estacionamiento" in Description and not strListInPhrase(noParkingKeys, Description):
        self.Parking = 1
    if self.Storage == 0:
      if "bodega" in Description:
        self.Storage = 1
    if self.Pool == 0:
      if "piscina" in Description:
        self.Pool = 1
    if self.Bdroom == default:
      BdroomsKeys = ["dormitorios", "ambientes", "cuartos", "piezas"]
      BdroomKeys = ["dormitorio", "ambiente", "cuarto", "pieza", "estudio", "studio"]
      if strOfListInPhrase(BdroomsKeys, Description):
        self.Bdroom = 2
      elif strOfListInPhrase(BdroomKeys, Description):
        self.Bdroom = 1
    if self.Bath == default:
      if "banos" in Description:
        self.Bath = 2
      elif "bano" in Description:
        self.Bath = 1
    
def build_portalinmobiliario(inDf, outCsvPath):
  default = -16
  # dateFormat = "%Y/%m/%d"
  with open(outCsvPath, 'a', encoding="utf-8") as outCsv:  
    for idx, row in inDf.iterrows():
      # INI house
      houseToWrite = buildHouse()
      # LINK values
      houseToWrite.linkSrce(row['Srce'])
      houseToWrite.linkRegion(row['Region'])
      houseToWrite.linkPropertyType(row['PropertyType'])
      houseToWrite.linkPropertyState(row['PropertyState'])
      houseToWrite.linkPublishedDate(row['PublishedDate'])
      houseToWrite.linkTitle(row['Title'])
      houseToWrite.linkSection(row['Section'])
      houseToWrite.linkModels(row['Models'])
      houseToWrite.linkfld0(row['fld0'])
      houseToWrite.linkfld1(row['fld1'])
      houseToWrite.linkfld2(row['fld2'])
      houseToWrite.linkfld3(row['fld3'])
      houseToWrite.linkfld4(row['fld4'])
      houseToWrite.linkfld5(row['fld5'])
      houseToWrite.linkFullPrice(row['FullPrice'])
      houseToWrite.linkLink(row['Link'])
      houseToWrite.linkDescription(row['Description'])

      # WRITE row
      outCsv.write(houseToWrite.toCsvRow())

# CSV processing
def buildCsv(srce, inCsvPath, outCsvPath):
  # Initial Vars
  inDf = pd.read_csv(inCsvPath)

  # SRCE switch
  if srce == "portal inmobiliario":
    build_portalinmobiliario(inDf, outCsvPath)
  else:
    print("srce {} is not supported".format(srce))
    exit()

# MAIN
def buildMain(log, snap, inCsvPaths, baseOutPath, statsPath, cols):
  # TIME start
  startTime, startStamp = getTimeAndStamp()
  # INIT
  currStep = "ETL_01_BUILD"
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
      outCsv.write(array2Str(cols, ',') + "\n")

    ## FORMAT
    buildCsv(
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
