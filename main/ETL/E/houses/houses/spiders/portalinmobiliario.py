# -*- coding: utf-8 -*-
"""
  @author: Brownbull - Gabriel Carcamo - carcamo.gabriel@gmail.com
  Scrapy base for web scrapping based on
  https://www.youtube.com/watch?v=ogPMCpcgb-E
"""
import imp
import scrapy
import pandas as pd
imp.load_source('program', "D:\Reference\python\include\program.py")
from program import *
# House Class
from houses.spiders.scrapHouse import scrapHouse


class PortalInmobiliarioSpyder(scrapy.Spider):
  name = 'portalinmobiliario'
  start_urls = []

  datasourcesPath = "D:\Reference\housemodel\data\ETL\E\\byHand\segment3_datasources.csv"
  inDf = pd.read_csv(datasourcesPath)
  for idx, row in inDf.iterrows():
    start_urls.append(row['Link'])

  cols = [ 
    "PriceUnit" ,
    "Price" ,
    "item" ,
    "link" 
    ]

  outCsvPath = "D:\Reference\housemodel\data\ETL\E\python\housesLinks.csv"
  with open(outCsvPath, 'w', encoding="utf-8") as outCsv:
    outCsv.write(array2Str(cols, ',') + "\n")

  def parse(self, response):
    waitSeconds = 1
    outCsvPath = "D:\Reference\housemodel\data\ETL\E\python\housesLinks.csv"

    housesLinks = response.xpath('/html/body/main/div[2]/div/section/ol/li/div/div[2]/a')

    with open(outCsvPath, 'a', encoding="utf-8") as outCsv:
      for houseLink in housesLinks:
        # Initialize House
        houseRow = scrapHouse()

        PriceUnit = houseLink.xpath('normalize-space(.//*[@class="price__symbol"]/text())')[0].extract()
        Price = houseLink.xpath('normalize-space(.//*[@class="price__fraction"]/text())')[0].extract()
        item = "{} {}".format(PriceUnit, Price)
        itemRef = houseLink.xpath('@href')[0].extract()
        houseRow.setPortalFields(PriceUnit , Price , item , itemRef)
        outCsv.write(houseRow.toCsvRowScrapy())

        next_page_url = response.xpath('//*[@class="andes-pagination__link prefetch"]/@href').extract_first()
        if next_page_url is not None:
          yield scrapy.Request(response.urljoin(next_page_url), callback=self.parse)
          
      # load_selector(response)
