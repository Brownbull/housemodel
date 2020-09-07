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

def initializeCsv1Col(filePath, col, force=False):
  if force:
    with open(filePath, 'w', encoding="utf-8") as csvFile:
      csvFile.write(col + "\n")
  else:
    if not Path(filePath).is_file():
      with open(filePath, 'w', encoding="utf-8") as csvFile:
        csvFile.write(col + "\n")
  return pd.read_csv(filePath)

# MAIN
def differentiateMain(log, snap, outCsvPath, statsPath, seeFilePath, seenFilePath, seenFilesPath, newFilePath, goneFilePath, dayDeleteFilePath, dayArchiveFilePath):
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
  
  # DATAFRAMES
  key = "link"
  cols = ["link"]
  seeDf = pd.read_csv(seeFilePath)
  # Initialize csv if they not exists
  seenDf = initializeCsv1Col(seenFilePath, key)
  deleteDf = initializeCsv1Col(dayDeleteFilePath, key)
  archivesDf = initializeCsv1Col(dayArchiveFilePath, key)

  # ROLLOUT on first day run
  seenFiles = os.listdir(seenFilesPath)
  logPrint(log, "Files for Diffs:\n\tsee File:\n\t\t{}".format(seeFilePath))

  # New day?
  if len(seenFiles) > 1:
    # Append seen to Archives
    archivesDf = joinConcat(seenDf, archivesDf, key, cols)
  else:
    # New Day, so New Archives only with what is new today
    archivesDf = joinLeftOnly(seeDf, archivesDf, key, cols)
    # New Day, so new delete file
    deleteDf = initializeCsv1Col(dayDeleteFilePath, key, force=True)

  # Get new offers and seen offers that are not present
  newDf = joinLeftOnly(seeDf, archivesDf, key, cols)
  oldDf = joinRightOnly(seeDf, archivesDf, key, cols)

  # update delete day file
  deleteDf = joinConcat(oldDf, deleteDf, key, cols)

  # WRITE csv
  newDf.to_csv(newFilePath, index=False)
  oldDf.to_csv(goneFilePath, index=False)
  deleteDf.to_csv(dayDeleteFilePath, index=False)
  archivesDf.to_csv(dayArchiveFilePath, index=False)

  # Save Files on date folder with time name
  newTimeFile = timeCp(newFilePath, outSnapPath + "/new/", pre="new_")
  goneTimeFile = timeCp(goneFilePath, outSnapPath + "/gone/", pre="gone_")
  # See file has been Seen
  shutil.copy2(seeFilePath, seenFilePath)

  # Append results for stats
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
  return outSnapPath


