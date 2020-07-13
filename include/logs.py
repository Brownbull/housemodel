# -*- coding: utf-8 -*-
"""
  @author: Brownbull - Gabriel Carcamo - carcamo.gabriel@gmail.com
  logs custom library
"""
import time
from datetime import datetime
from include.files import *

def logPrint(logPath, msg):
  now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
  logMsg = "{}: {}". format(now, msg)
  print("# " + logMsg)
  with open(logPath, 'a', encoding="utf-8") as log:
    log.write(logMsg + "\n")

def logInit(path, prefix):
  now = datetime.now().strftime('%Y%m%d_%H.%M.%S')
  logName = prefix + "@" + now + ".log"
  return initFilePath(path, logName)