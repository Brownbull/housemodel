# -*- coding: utf-8 -*-
"""
  @author: Brownbull - Gabriel Carcamo - carcamo.gabriel@gmail.com
  Extract - Extract Module inside Transform on ETL for houses model
"""
# IMPORT LIBRARIES
import scrapy
from scrapy.crawler import CrawlerProcess
from main.ETL.E.houses.scrapy.portalinmobiliario import PortalInmobiliarioSpyder
from main.include.logs import *
from main.include.files import *
from main.include.program import *
from main.ETL.stats import *

def callCrawler():
  # https://stackoverflow.com/questions/13437402/how-to-run-scrapy-from-within-a-python-script
  process = CrawlerProcess({
      'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
  })
  process.crawl(PortalInmobiliarioSpyder)
  process.start() # the script will block here until the crawling is finished
  return "D:\Reference\housemodel\data\ETL\E\python\see.csv"

# MAIN
def seeMain(log, snap, seeFilePath, outCsvPath, statsPath):
  # TIME start
  startTime, startStamp = getTimeAndStamp()
  # INIT
  currStep = "ETL_E_00_SEE"
  logPrint(log, "{} Start: {}".format(currStep, str(startStamp)))

  # SET
  outSnapPath = outCsvPath + "/" + snap + "/portalinmobiliario/" +  currStep
  statsSnapPath = statsPath + "/" + snap

  # SCRAPY
  scrapyOut = callCrawler()
  logPrint(log, "Scrapy executed, file {} created.".format(scrapyOut))

  # SEE file
  pdCsvSort(seeFilePath, "link", dedup=True)
  seeTimeFile = timeCp(seeFilePath, outSnapPath, ext = "csv")
  logPrint(log, "New time associated file: {}".format(seeTimeFile))

  ## FINISH
  # END TIMING & LOG
  endTime, endStamp = getTimeAndStamp()
  logPrint(log, "{} End: {}".format(currStep, str(endStamp)))
  logPrint(log, "{} Total Execution Time: {}".format(currStep, str(endTime - startTime)))
  ## STATS
  statsSnapStepPath = statsSnapPath + "/" + currStep
  stepEnd(log, statsSnapStepPath, currStep, [seeTimeFile])
  return outSnapPath


