# -*- coding: utf-8 -*-
"""
  @author: Brownbull - Gabriel Carcamo - carcamo.gabriel@gmail.com
  Files Functions Module
"""
import os
import sys
import yaml
import time
import shutil
import pandas as pd
from datetime import datetime
from pathlib import Path

def initPath(path):
  if not Path(path).exists():
    os.makedirs(path)

def initFilePath(path, file):
  initPath(path)
  filePath = path + "/" + file
  if Path(filePath).is_file():
    os.remove(filePath)
  return filePath

def getRawFileName(Path):
  return (os.path.splitext(os.path.basename(Path))[0])

# YAML CONSTRUCTORS
def join(loader, node):
  seq = loader.construct_sequence(node)
  return ''.join([str(i) for i in seq])

def readConfg(fConfig):
  # init yaml functions
  yaml.add_constructor('!join', join)
  # read yaml
  if Path(fConfig).is_file():
    with open(fConfig, 'r', encoding="utf-8") as configFile:
      return yaml.load(configFile)
  else:
    sys.exit('Error: File ' + fConfig + " was not found.")

def file2Folder(fileIn, fileOutPath, debug=False):
  if debug:
    print("file2Folder: {} --> {}".format(fileIn, fileOutPath))
  shutil.copy2(fileIn, fileOutPath)

def file2File(fileIn, fileOut, debug=False):
  if debug:
    print("file2File: {} --> {}".format(fileIn, fileOut))
  shutil.copy2(fileIn, fileOut)
  return fileOut

def timeCp(fileIn, fileOutPath, ext="csv", pre=""):
  now = datetime.now().strftime('%H.%M.%S')
  fileName = pre + now + "." + ext
  initFilePath(fileOutPath, fileName)
  fileOut = fileOutPath + "/" + fileName
  shutil.copy2(fileIn, fileOut)
  return fileOut

def dayTimeCp(fileIn, fileOutPath, ext="csv", pre=""):
  today = datetime.now().strftime('%Y%m%d')
  fileOutPath = fileOutPath + today + "/"
  now = datetime.now().strftime('%H.%M.%S')
  fileName = pre + now + "." + ext
  initFilePath(fileOutPath, fileName)
  fileOut = fileOutPath + fileName
  shutil.copy2(fileIn, fileOut)
  return fileOut

def pdCsvSort(filePath, key):
  df = pd.read_csv(filePath)
  df = df.sort_values(key)
  df.to_csv(filePath, index=False)