# -*- coding: utf-8 -*-
"""
  @author: Brownbull - Gabriel Carcamo - carcamo.gabriel@gmail.com
  DASH - Dashboard Module
"""
# IMPORT LIBRARIES
from main.DASH.imports import *
from main.include.logs import *
from main.include.files import *
from main.include.program import *

# CHECK ARGUMENTS
parser = argparse.ArgumentParser(description='Main process of Dashboards Module.')
parser.add_argument('-dashConfig','-dshc', '-c', help='Dashboards Config File Path', default="config/DASH.yaml")
args = parser.parse_args()

# READ CONFIG FILE
dashCfg = readConfg(args.dashConfig)
if dashCfg['debug']: print(dashCfg)

# LIBS CHECK
if dashCfg['debug']:
  print("# Imported Libraries:") 
  getVersions()
  print("# Debug Options:\n{}".format(args))

# LOAD DASH LIBS
imp.load_source('groupLib', dashCfg['groupLib'])
from groupLib import *

# INITIALIZE TIMING & LOG
startTime, startStamp = getTimeAndStamp()
log = logInit("logs", "DASH")
logPrint(log, "DASH Start: {}".format(str(startStamp)))

## DASH Step - GROUP
houseMarketPath = groupHouseMarket(
  log,
  dashCfg['selectBaseTable'],
  dashCfg['createMarketView'],
  dashCfg['selectMarketView'],
  dashCfg['dataFolder'],
  dashCfg['statsPath'],
  dashCfg['dbCfg']
)

## DASH Step - SHOW

              
# END TIMING & LOG
endTime, endStamp = getTimeAndStamp()
logPrint(log, "DASH End: {}".format(str(endStamp)))
logPrint(log, "DASH Total Execution Time: {}".format(str(endTime - startTime)))