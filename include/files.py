# -*- coding: utf-8 -*-
"""
  @author: Brownbull - Gabriel Carcamo - carcamo.gabriel@gmail.com
  Files Functions Module
"""
import os
import sys
import yaml
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


