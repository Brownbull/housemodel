# -*- coding: utf-8 -*-
"""
  @author: Brownbull - Gabriel Carcamo - carcamo.gabriel@gmail.com
  Extract - Extract Module inside Transform on ETL for houses model
"""
# IMPORT LIBRARIES
import pandas as pd
from main.include.logs import *
from main.include.files import *
from main.include.common import *
from main.include.program import *
from main.ETL.stats import *

# https://stackoverflow.com/questions/28901683/pandas-get-rows-which-are-not-in-other-dataframe
def getDiffs(log, seeFile, seenFile, newFile, goneFile):
  logPrint(log, "Files for Diffs:\n\tsee File: {}\n\tseen File:{}".format(seeFile, seenFile))
  seeDf = pd.read_csv(seeFile)
  seenDf = pd.read_csv(seenFile)

  # Get houses that you see now but not before
  dfNew = seeDf.merge(seenDf.drop_duplicates(), on=['link'], 
                    how='left', indicator=True)
  dfNew['_merge'] == 'left_only'
  dfNew = dfNew[dfNew['_merge'] == 'left_only']

  # Get houses that you saw before but not now
  dfGone = seeDf.merge(seenDf.drop_duplicates(), on=['link'], 
                    how='right', indicator=True)
  dfGone = dfGone[dfGone['_merge'] == 'right_only']

  # WRITE csv
  dfNew.to_csv(newFile, index=False)
  dfGone.to_csv(goneFile, index=False)

# MAIN
def differentiateMain(log, snap, outCsvPath, statsPath, seeFile, seenFile, newFile, goneFile):
  dbg = False

  # TIME start
  startTime, startStamp = getTimeAndStamp()
  # INIT
  currStep = "ETL_E_01_DIFFERENTIATE"
  logPrint(log, "{} Start: {}".format(currStep, str(startStamp)))

  # SET
  outSnapPath = outCsvPath + "/" + snap + "/portalinmobiliario/" + currStep
  statsSnapPath = statsPath + "/" + snap
  # OUTPUT setup
  initPath(outSnapPath)
  outDifferentiateFiles = []
  # outCsvPath = initFilePath(outSnapPath, "allHouses.csv")

  # ROLLOUT on first run
  if not Path(seenFile).is_file():
    logPrint(log, "seenFile does not exist, seeFile will be copied as seenFile")
    file2File(seeFile, seenFile, debug=dbg)

  # get DIFFS from see and seen elements
  getDiffs(log, seeFile, seenFile, newFile, goneFile)

  # Save Files on date folder with time name
  newTimeFile = timeCp(newFile, outSnapPath + "/new/", pre="new_")
  goneTimeFile = timeCp(goneFile, outSnapPath + "/gone/", pre="gone_")
  outDifferentiateFiles.append(newTimeFile)
  outDifferentiateFiles.append(goneTimeFile)

  ## FINISH
  # END TIMING & LOG
  endTime, endStamp = getTimeAndStamp()
  logPrint(log, "{} End: {}".format(currStep, str(endStamp)))
  logPrint(log, "{} Total Execution Time: {}".format(currStep, str(endTime - startTime)))
  ## STATS
  statsSnapStepPath = statsSnapPath + "/" + currStep
  stepEnd(log, statsSnapStepPath, currStep, outDifferentiateFiles)
  return outDifferentiateFiles


