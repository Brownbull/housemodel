# -*- coding: utf-8 -*-
"""
  @author: Brownbull - Gabriel Carcamo - carcamo.gabriel@gmail.com
  ETL - Extract Transform Load Module
"""
# IMPORT LIBRARIES
from main.ETL.imports import *
from main.include.logs import *
from main.include.files import *
from main.include.program import *

# CHECK ARGUMENTS
parser = argparse.ArgumentParser(description='Main process of Transform Module.')
parser.add_argument('-etlConfig','-etlc', '-c', help='Tranform Config File Path', default="config/ETL.yaml")
parser.add_argument('-sample','-sm', '-sp', required=False, help='Numbers of records to take as sample', default=0)
args = parser.parse_args()

# READ CONFIG FILE
etlCfg = readConfg(args.etlConfig)
if etlCfg['debug']: print(etlCfg)

# LIBS CHECK
if etlCfg['debug']:
  print("# Imported Libraries:") 
  getVersions()
  print("# Debug Options:\n{}".format(args))

# EXTRACT LIBS
imp.load_source('seeLib', etlCfg['seeLib'])
from seeLib import *
imp.load_source('differentiateLib', etlCfg['differentiateLib'])
from differentiateLib import *
# TRANSFORM LIBS
imp.load_source('collectLib', etlCfg['collectLib'])
from collectLib import *
imp.load_source('buildLib', etlCfg['buildLib'])
from buildLib import *
imp.load_source('cleanLib', etlCfg['cleanLib'])
from cleanLib import *
imp.load_source('formatLib', etlCfg['formatLib'])
from formatLib import *
imp.load_source('fillLib', etlCfg['fillLib'])
from fillLib import *
imp.load_source('fEngLib', etlCfg['fEngLib'])
from fEngLib import *
# LOAD LIBS
imp.load_source('loadLib', etlCfg['loadLib'])
from loadLib import *

# INITIALIZE TIMING & LOG
startTime, startStamp = getTimeAndStamp()
log = logInit("logs", "ETL")
logPrint(log, "ETL Start: {}".format(str(startStamp)))

# GET INPUT DATA INDEX
dataIndex = pd.read_csv(etlCfg['dataIndex'])

# MAIN
## Create SNAPSHOT
if etlCfg['version'] > 2:
  snap = datetime.now().strftime('%Y%m%d')
  logPrint(log, "Version {} detected".format(etlCfg['version']))
  logPrint(log, "Creating Snapshot for {}".format(snap))
  ## ETL E Step - SEE
  seeFile = seeMain(
    log, 
    snap, 
    etlCfg['dataExtract'], 
    etlCfg['statsPath'])
  
  # seeFile = "D:/Reference/housemodel/data/ETL/E/python/20200831/portalinmobiliario/ETL_E_00_SEE/20.36.13.csv"

  ## ETL E Step - APPEND
  diffsFile = differentiateMain(
    log, 
    snap, 
    etlCfg['dataExtract'], 
    etlCfg['statsPath'],
    seeFile,
    etlCfg['seenFile'],
    etlCfg['newFile'],
    etlCfg['goneFile'])

  exit()
  snapshots = os.listdir(etlCfg['dataFolder'] + "/" + snap)
else:
  # GET AVAILABLE DATA SNAPSHOTS
  snapshots = os.listdir(etlCfg['dataFolder'])
  
logPrint(log, "Available Snapshots: " + array2Str(snapshots, ' '))
exit()

# MAIN
## iterate SNAPSHOTS
for snap in snapshots:
  if (int(etlCfg['procDtIni']) <= int(snap) <= int(etlCfg['procDtEnd'])) or etlCfg['version'] > 2:
    logPrint(log, "Processing Snapshot: {}".format(snap))

    ## ETL T Step - COLLECT
    collectFiles = collectMain(
      log, 
      snap, 
      dataIndex, # index of sources
      etlCfg['dataFolder'], # baseInPath
      etlCfg['transformPath'], # baseOutPath
      etlCfg['statsPath'], # statsPath
      etlCfg['dataSrces'], 
      etlCfg['collectionCols'])
    # input("After COLLECT: Press Enter to continue...")
              
    ## ETL T Step - BUILD
    buildFiles = buildMain(
      log, 
      snap, 
      collectFiles, 
      etlCfg['transformPath'], # baseOutPath
      etlCfg['statsPath'], # statsPath
      etlCfg['buildCols'])
    # input("After BUILD: Press Enter to continue...")

    ## ETL T Step - CLEAN
    cleanFiles = cleanMain(
      log, 
      snap, 
      buildFiles, 
      etlCfg['transformPath'], # baseOutPath
      etlCfg['statsPath'], # statsPath
      etlCfg['cleanCols'])
    # input("After CLEAN: Press Enter to continue...")
              
    ## ETL T Step - FORMAT
    formatFiles = formatMain(
      log, 
      snap, 
      cleanFiles, 
      etlCfg['transformPath'], # baseOutPath
      etlCfg['statsPath'], # statsPath
      etlCfg['formatCols'])
    # input("After FORMAT: Press Enter to continue...")
              
    ## ETL T Step - FILL
    fillFiles = fillMain(
      log, 
      snap, 
      formatFiles,
      etlCfg['transformPath'], # baseOutPath
      etlCfg['statsPath']) # statsPath
    # input("After FILL: Press Enter to continue...")

    ## ETL T Step - FEATURE ENG
    fEngFiles = fEngMain(
      log,
      snap, 
      fillFiles,
      etlCfg['transformPath'], # baseOutPath
      etlCfg['statsPath'], # statsPath
      etlCfg['fEngCols'])
    # input("After FEATURE: Press Enter to continue...")

    ## ETL L Step - LOAD
    loadMain(
      log,
      snap,
      fEngFiles,
      etlCfg['transformPath'], # baseOutPath
      etlCfg['statsPath'], # statsPath
      etlCfg['fEngCols'],
      etlCfg['dbCfg'])
    # input("After LOAD: Press Enter to continue...")
              
# END TIMING & LOG
endTime, endStamp = getTimeAndStamp()
logPrint(log, "ETL End: {}".format(str(endStamp)))
logPrint(log, "ETL Total Execution Time: {}".format(str(endTime - startTime)))