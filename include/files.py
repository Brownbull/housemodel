# -*- coding: utf-8 -*-
"""
  @author: Brownbull - Gabriel Carcamo - carcamo.gabriel@gmail.com
  Files Functions Module
"""
import os
from pathlib import Path

def initPath(path):
  if not Path(path).exists():
    os.makedirs(path)

def initFileInPath(path, file):
  initPath(path)
  filePath = path + "/" + file
  if Path(filePath).is_file():
    os.remove(filePath)
  return filePath