# -*- coding: utf-8 -*-
"""
  @author: Brownbull - Gabriel Carcamo - carcamo.gabriel@gmail.com
  DASH - Dashboard Module
"""
# IMPORT LIBRARIES
from main.DASH.imports import *
from include.logs import *
from include.files import *
from include.program import *

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


  

# INITIALIZE TIMING & LOG
startTime, startStamp = getTimeAndStamp()
log = logInit("logs", "DASH")
logPrint(log, "DASH Start: {}".format(str(startStamp)))













              
# END TIMING & LOG
endTime, endStamp = getTimeAndStamp()
logPrint(log, "DASH End: {}".format(str(endStamp)))
logPrint(log, "DASH Total Execution Time: {}".format(str(endTime - startTime)))