# -*- coding: utf-8 -*-
"""
  @author: Brownbull - Gabriel Carcamo - carcamo.gabriel@gmail.com
  scrapLinks Module
"""
import scrapy
from scrapy.crawler import CrawlerProcess
from include.program import *
from include.files import *
from main.ETL.E.houses.portalinmobiliario import PortalInmobiliarioSpyder


# https://stackoverflow.com/questions/13437402/how-to-run-scrapy-from-within-a-python-script
process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
})
process.crawl(PortalInmobiliarioSpyder)
process.start() # the script will block here until the crawling is finished

scrapyOut = "D:\Reference\housemodel\data\ETL\E\python\housesLinks.csv"
outPath = "data\ETL\E\python/"

print(dayTimeCp(scrapyOut, outPath, ext = "csv"))
# return initFilePath(logPath, logName)


