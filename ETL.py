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

# LOAD TRANSFORM LIB
imp.load_source('etlTransformLib', etlCfg['etlTransformLib'])
from etlTransformLib import *

# INITIALIZE TIMING & LOG
startTime, startStamp = getTimeAndStamp()
log = logInit("logs", "ETL")
logPrint(log, "ETL Start: {}".format(str(startStamp)))

# GET INPUT DATA INDEX
dataIndex = pd.read_csv(etlCfg['dataIndex'])

# GET AVAILABLE DATA SNAPSHOTS
snapshots = os.listdir(etlCfg['dataFolder'])
logPrint(log, "Available Snapshots: " + array2Str(snapshots, '-'))

# PROCESS 
## SNAPSHOTS
for snap in snapshots:
  if int(etlCfg['procDtIni']) <= int(snap) <= int(etlCfg['procDtEnd']):
    logPrint(log, "Processing Snapshot: {}".format(snap))
    currSnap = etlCfg['dataFolder'] + "/" + snap + "/"
    ## SOURCES
    for srce in etlCfg['dataSrces']:
      logPrint(log, "Processing Source: {}".format(srce))

      # Set Outfile
      outPath = etlCfg['etlTransformPath'] + "/" + snap
      outFile = initFilePath(outPath, srce + ".csv")
      # WRITE HEADER
      with open(outFile, 'w') as outCsv:
        outCsv.write(array2Str(etlCfg['etlTransformCols'], ',') + "\n")

      # Get Available csv files
      currSnapSrce = currSnap + "/" + srce + "/"
      csvFiles = os.listdir(currSnapSrce)

      ## CSV
      for csv in csvFiles:
        # Get Parms to process single csv into outPath file
        currSnapSrceCsvPath = currSnapSrce + csv
        csvId = csv.split('.')[0]
        idxRow = dataIndex.iloc[int(csvId)-1]
        ## TRANSFORM
        etlTransform(outFile, snap, srce, idxRow, currSnapSrceCsvPath)
        exit()

        



# GET INPUT DATA
# if args.sample:
#   houses_raw = pd.read_csv(args.enrolls, nrows = args.sample)
# else:
#   houses_raw = pd.read_csv(args.enrolls) 

# # end stage
# finishedStage = "ETL_01_GET_RAW"
# stageEndSet(finishedStage, dfs, args.info, args.debug)








# END TIMING & LOG
endTime, endStamp = getTimeAndStamp()
logPrint(log, "ETL End: {}".format(str(endStamp)))
logPrint(log, "Total Execution: {}".format(str(endTime - startTime)))