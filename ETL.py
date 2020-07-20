# -*- coding: utf-8 -*-
"""
  @author: Brownbull - Gabriel Carcamo - carcamo.gabriel@gmail.com
  ETL - Extract Transform Load Module
"""
# IMPORT LIBRARIES
from main.ETL.imports import *
from include.logs import *
from include.files import *
from include.program import *

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

# LOAD TRANSFORM LIBS
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
imp.load_source('loadLib', etlCfg['loadLib'])
from loadLib import *

# INITIALIZE TIMING & LOG
startTime, startStamp = getTimeAndStamp()
log = logInit("logs", "ETL")
logPrint(log, "ETL Start: {}".format(str(startStamp)))

# GET INPUT DATA INDEX
dataIndex = pd.read_csv(etlCfg['dataIndex'])

# GET AVAILABLE DATA SNAPSHOTS
snapshots = os.listdir(etlCfg['dataFolder'])
logPrint(log, "Available Snapshots: " + array2Str(snapshots, ' '))

# MAIN
## iterate SNAPSHOTS
for snap in snapshots:
  if int(etlCfg['procDtIni']) <= int(snap) <= int(etlCfg['procDtEnd']):
    logPrint(log, "Processing Snapshot: {}".format(snap))

    ## ETL Step - COLLECT
    collectFiles = collectMain(
      log, 
      snap, 
      dataIndex, # index of sources
      etlCfg['dataFolder'], # baseInPath
      etlCfg['transformPath'], # baseOutPath
      etlCfg['statsPath'], # statsPath
      etlCfg['dataSrces'], 
      etlCfg['collectionCols'])
              
    ## ETL Step - BUILD
    buildFiles = buildMain(
      log, 
      snap, 
      collectFiles, 
      etlCfg['transformPath'], # baseOutPath
      etlCfg['statsPath'], # statsPath
      etlCfg['buildCols'])
    exit()

    ## ETL Step - CLEAN
    cleanFiles = cleanMain(
      log, 
      etlCfg['transformPath'], # baseOutPath
      etlCfg['statsPath'], # statsPath
      snap, 
      buildFiles, 
      etlCfg['cleanCols'])
    exit()
              
    ## ETL Step - FORMAT
    formatFiles = formatMain(
      log, 
      etlCfg['transformPath'], # baseOutPath
      etlCfg['statsPath'], # statsPath
      snap, 
      cleanFiles, 
      etlCfg['formatCols'])
              
    ## ETL Step - FILL
    fillFiles = fillMain(
      log, 
      etlCfg['transformPath'], # baseOutPath
      etlCfg['statsPath'], # statsPath
      snap, 
      formatFiles)

    ## ETL Step - FEATURE ENG
    fEngFiles = fEngMain(
      log,
      etlCfg['transformPath'], # baseOutPath
      etlCfg['statsPath'], # statsPath
      snap, 
      fillFiles,
      etlCfg['fEngCols'])

    ## ETL Step - LOAD
    loadMain(
      log,
      etlCfg['transformPath'], # baseOutPath
      etlCfg['statsPath'], # statsPath
      snap, 
      fEngFiles,
      etlCfg['fEngCols'],
      etlCfg['dbCfg'])
              


# END TIMING & LOG
endTime, endStamp = getTimeAndStamp()
logPrint(log, "ETL End: {}".format(str(endStamp)))
logPrint(log, "ETL Total Execution Time: {}".format(str(endTime - startTime)))