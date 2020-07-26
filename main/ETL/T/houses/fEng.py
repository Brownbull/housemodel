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

def getSizeGroup(Mt2):
  if 10000 < Mt2:
    return "GT10K"
  if 5000 < Mt2 <= 10000:
    return "5KTO10K"
  if 1000 < Mt2 <= 5000:
    return "1KTO5K"
  if 500 < Mt2 <= 1000:
    return "5HTO1K"
  if 400 < Mt2 <= 500:
    return "4HTO5H"
  if 300 < Mt2 <= 400:
    return "3HTO4H"
  if 250 < Mt2 <= 300:
    return "250TO3H"
  if 200 < Mt2 <= 250:
    return "2HTO250"
  if 190 < Mt2 <= 200:
    return "190TO2H"
  if 180 < Mt2 <= 190:
    return "180TO190"
  if 170 < Mt2 <= 180:
    return "170TO180"
  if 160 < Mt2 <= 170:
    return "160TO170"
  if 150 < Mt2 <= 160:
    return "150TO160"
  if 140 < Mt2 <= 150:
    return "140TO150"
  if 130 < Mt2 <= 140:
    return "130TO140"
  if 120 < Mt2 <= 130:
    return "120TO130"
  if 110 < Mt2 <= 120:
    return "110TO120"
  if 100 < Mt2 <= 110:
    return "100TO110"
  if 90 < Mt2 <= 100:
    return "90TO100"
  if 80 < Mt2 <= 90:
    return "80TO90"
  if 70 < Mt2 <= 80:
    return "80TO90"
  if 60 < Mt2 <= 70:
    return "80TO90"
  if 50 < Mt2 <= 60:
    return "80TO90"
  if 40 < Mt2 <= 50:
    return "80TO90"
  if 30 < Mt2 <= 40:
    return "80TO90"
  else:
    return "LT30"

def sectorPoints(sector, default=-16):
  if sector == "santiago":
    return 4
  elif sector == "huechuraba":
    return 3
  elif sector == "nunoa":
    return 4
  elif sector == "providencia":
    return 5
  elif sector == "las condes":
    return 5
  elif sector == "vitacura":
    return 5
  elif sector == "lo barnechea":
    return 3
  elif sector == "la florida":
    return 2
  elif sector == "maipu":
    return 1
  elif sector == "penalolen":
    return 2
  elif sector == "puente alto":
    return 1
  elif sector == "estacion central":
    return 1
  elif sector == "quinta normal":
    return 1
  elif sector == "lo prado":
    return 1
  elif sector == "lo conchali":
    return 2
  elif sector == "quilicura":
    return 1
  elif sector == "macul":
    return 2
  elif sector == "la reina":
    return 3
  elif sector == "penaflor":
    return 1
  elif sector == "colina":
    return 1
  elif sector == "pirque":
    return 1
  elif sector == "independencia":
    return 2
  elif sector == "pudahuel":
    return 1
  elif sector == "recoleta":
    return 2
  elif sector == "renca":
    return 1
  elif sector == "lampa":
    return 1
  elif sector == "paine":
    return 1
  elif sector == "tiltil":
    return 1
  elif sector == "buin":
    return 1
  elif sector == "melipilla":
    return 1
  elif sector == "el monte":
    return 1
  elif sector == "curacavi":
    return 1
  elif sector == "cerrillos":
    return 1
  elif sector == "talagante":
    return 1
  elif sector == "la granja":
    return 1
  elif sector == "maria pinto":
    return 1
  elif sector == "padre hurtado":
    return 1
  elif sector == "la cisterna":
    return 1
  elif sector == "san miguel":
    return 2
  elif sector == "san bernardo":
    return 1
  elif sector == "el bosque":
    return 1
  elif sector == "la pintana":
    return 1
  elif sector == "san joaquin":
    return 1
  elif sector == "san ramon":
    return 1
  elif sector == "cerro navia":
    return 1
  elif sector == "pedro aguirre cerda":
    return 1
  elif sector == "calera de tango":
    return 1
  elif sector == "san jose de maipo":
    return 1
  elif sector == "isla de maipo":
    return 1
  else:
    return default

def getValue(row, default=-16):
  Bdroom = row['Bdroom'] * 4
  Bath = row['Bath'] * 4
  Balcony = row['Balcony'] * 1
  Parking = row['Parking'] * 3
  Storage = row['Storage'] * 2
  Pool = row['Pool'] * 1
  Sector = row['Sector'] * 1.5
  MtTot = (row['MtTot'] * 0.2) if row['MtTot'] != default else 0
  MtUtil = (row['MtUtil'] * 0.3) if row['MtUtil'] != default else 0
  PropertyType = row['PropertyType']

  if PropertyType == 'aparment':
    return int((Bdroom + Bath + Balcony + Parking + Storage + Pool + Sector) * (MtTot + MtUtil))
  else:
    return int((Bdroom + Bath + Balcony + Parking + Storage + Pool + Sector) * (MtTot + MtUtil * 0.7))

def getUFxMt2(row, default=-16):
  if row['MtUtil'] != default and row['MtTot'] != default:
    return round(row['PriceUF']/min(row['MtUtil'], row['MtTot']), 0)
  elif row['MtUtil'] != default:
    return round(row['PriceUF']/row['MtUtil'], 0)
  elif row['MtTot'] != default:
    return round(row['PriceUF']/row['MtTot'], 0)
  else:
    return default

# CSV processing
def fEngCsv(snap, srce, inCsvPath, outCsvPath, cols):
  default = -16
  minPropertyArea = 15 # 15 mt2
  # Initial Vars
  inDf = pd.read_csv(inCsvPath)

  # FROM CSV
  for idx, row in inDf.iterrows():
    inDf.loc[idx, 'Balcony'] = 0
    if row['MtTot'] != default and row['MtUtil'] != default and row['MtTot'] > minPropertyArea and row['MtUtil'] > minPropertyArea:
      if row['MtTot'] > row['MtUtil'] > minPropertyArea and row['PropertyType'] == 'aparment':
        inDf.loc[idx, 'Balcony'] = 1
      inDf.loc[idx, 'SizeGroup'] = getSizeGroup(row['MtUtil'])
    else:
      if row['MtUtil'] != default:
        inDf.loc[idx, 'SizeGroup'] = getSizeGroup(row['MtUtil'])
      else:
        inDf.loc[idx, 'SizeGroup'] = getSizeGroup(row['MtTot'])

    inDf.loc[idx, 'Sector'] = sectorPoints(row['Province'])
    inDf.loc[idx, 'UFxMt2'] = getUFxMt2(row)
    inDf.loc[idx, 'Value'] = getValue(inDf.loc[idx])

  # NEW cols
  inDf['SnapDate'] = inDf.apply(lambda row: snap, axis=1)
  inDf['Score'] = inDf.apply(lambda row: int(row['Value']*1000/row['PriceUF']), axis=1)

  # DROP outliers rows
  # inDf.drop(inDf[inDf.Score > 4000].index, inplace=True)

  # Reorder Columns
  inDf = inDf[cols]

  # WRITE csv
  inDf.to_csv(outCsvPath, index=False)

# MAIN
def fEngMain(log, snap, inCsvPaths, baseOutPath, statsPath, cols):
  # TIME start
  startTime, startStamp = getTimeAndStamp()
  # INIT
  currStep = "ETL_05_FEATURE_ENG"
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
