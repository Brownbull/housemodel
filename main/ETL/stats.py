# -*- coding: utf-8 -*-
"""
  @author: Brownbull - Gabriel Carcamo - carcamo.gabriel@gmail.com
  Stats Module for ETL
"""

import pandas as pd
from main.include.logs import *
from main.include.files import *
from main.include.program import *

def stepEnd(log, outPath, stepName, csvFilePaths):
  for csvPath in csvFilePaths:
    csvName = getRawFileName(csvPath)
    # get DF
    df = pd.read_csv(csvPath) 
    dfStats(log, outPath, stepName, df, csvName)

def dfStats(log, outPath, stepName, df, csvName):
  # SET WRITE DIRECTORY
  outDir = outPath + "/" + csvName
  initPath(outDir)

  # START
  logPrint(log, "{} Creating stats files for {}".format(stepName, csvName)) 

  # INFO
  fInfo = open(outDir + "/info.txt", 'w+')
  df.info(buf=fInfo)
  fInfo.close()
  logPrint(log, "\tInfo:\t\t" + outDir +"/info.txt")

  # DESCRIBE
  df.describe(include = 'all').to_csv(outDir +"/describe.csv")
  logPrint(log, "\tDescribe:\t" + outDir +"/describe.csv")

  # NULLS
  fNull = open(outDir +"/nulls.txt", 'w+')
  fNull.write(csvName + ' columns with null values:\n')
  nulls = df.isnull().sum()
  for key,value in nulls.iteritems():
    # https://stackoverflow.com/questions/8234445/python-format-output-string-right-alignment
    fNull.write('{:>30}  {:>20}\n'.format(key, str(value)))
  fNull.close()
  logPrint(log, "\tNulls:\t\t" + outDir +"/nulls.txt")

  # 0s
  fNull = open(outDir +"/ceros.txt", 'w+')
  fNull.write(csvName + ' columns with 0 values:\n')
  ceros = (df == 0).sum(axis=0)
  for key,value in ceros.iteritems():
    fNull.write('{:>30}  {:>20}\n'.format(key, str(value)))
  fNull.close()
  logPrint(log, "\tCeros:\t\t" + outDir + "/ceros.txt")
