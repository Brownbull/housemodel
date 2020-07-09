# -*- coding: utf-8 -*-
"""
  @author: Brownbull - Gabriel Carcamo - carcamo.gabriel@gmail.com
  logs custom library
"""
import time
from datetime import datetime
from include.logs import *

def timerPlay(dbg):
  timeStart = time.time()
  dtStart = datetime.fromtimestamp(timeStart)
  if dbg:
    print("\nPreprocessing Script Start: " + str(dtStart) + "\n" + "-"*25 )

def logPrint(logPath, msg):
  now = datetime.now().strftime('%Y-%m-%d %M:%S.%f')[:-4]
  logMsg = "{}: {}". format(now, msg)
  print(logMsg)
  with open(logPath, 'w') as log:
    log.write(logMsg)