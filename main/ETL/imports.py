# -*- coding: utf-8 -*-
"""
  @author: Brownbull - Gabriel Carcamo - carcamo.gabriel@gmail.com
  Imports for Transform Module
"""
# misc libraries
import os
import imp
import sys #access to system parameters https://docs.python.org/3/library/sys.html
import argparse #read command line arguments on script calling
import random
import json
from pathlib import Path
from datetime import datetime
from include.program import *

# processing
import pandas as pd #collection of functions for data processing and analysis modeled after R dataframes with SQL like features

#ignore warnings
import warnings
warnings.filterwarnings('ignore')

def getVersion(lib):
  print("{} version: {}". format(retrieveName(lib), lib.__version__))

def getVersions():
  print("Python version: {}". format(sys.version))
  getVersion(pd)
