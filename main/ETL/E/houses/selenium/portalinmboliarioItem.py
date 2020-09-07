# -*- coding: utf-8 -*-
"""
  @author: Brownbull - Gabriel Carcamo - carcamo.gabriel@gmail.com
  Selenium scripts based on https://www.youtube.com/watch?v=Xjv1sY630Uc

  Initial Setup:
  pip install selenium
  # download chrome web driver: 
  #   Options-> Help -> About -> get version 
  #     Version 84.0.4147.105 (Official Build) (64-bit)
  #   go to https://sites.google.com/a/chromium.org/chromedriver/downloads
  #   download the driver
"""
# IMPORTS
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys # keyboard keys: enter, space, etc.
import time
import math
import pandas as pd

def checkElementByCSSSelector(driver, cssSel, waitSeconds):
  try:
    element = WebDriverWait(driver, waitSeconds).until(
      EC.presence_of_element_located((By.CSS_SELECTOR, cssSel))
    )
    # element = driver.find_element_by_css_selector(cssSel)
    return element.text
  except:
    return None

def getItemFields(driver, waitSeconds, inDf, outCsvPath):
  for idx, row in inDf.iterrows():
    print("Processing item: {}".format(idx))
    # Open
    driver.get(row['link'])
    # Get Fields
    inDf.loc[idx,'PriceUnit'] = checkElementByCSSSelector(driver, ".price-tag-motors span.price-tag-symbol", waitSeconds)
    inDf.loc[idx,'Price'] = checkElementByCSSSelector(driver, ".price-tag-motors span.price-tag-fraction", waitSeconds)
    inDf.loc[idx,'fld0'] = checkElementByCSSSelector(driver, "li.specs-item:nth-of-type(1)", waitSeconds)
    inDf.loc[idx,'fld1'] = checkElementByCSSSelector(driver, "li.specs-item:nth-of-type(2)", waitSeconds)
    inDf.loc[idx,'fld2'] = checkElementByCSSSelector(driver, "li.specs-item:nth-of-type(3)", waitSeconds)
    inDf.loc[idx,'fld3'] = checkElementByCSSSelector(driver, "li.specs-item:nth-of-type(4)", waitSeconds)
    inDf.loc[idx,'fld4'] = checkElementByCSSSelector(driver, "li.specs-item:nth-of-type(5)", waitSeconds)
    inDf.loc[idx,'fld5'] = checkElementByCSSSelector(driver, "li.specs-item:nth-of-type(6)", waitSeconds)
    inDf.loc[idx,'Description'] = checkElementByCSSSelector(driver, "section.item-description", waitSeconds)
    inDf.loc[idx,'PropertyState'] = checkElementByCSSSelector(driver, "li:nth-of-type(3) a.breadcrumb", waitSeconds)
    inDf.loc[idx,'PublishedDate'] = checkElementByCSSSelector(driver, ".info-property-date p.info", waitSeconds)
    inDf.loc[idx,'Title'] = checkElementByCSSSelector(driver, "h1", waitSeconds)
    inDf.loc[idx,'Section'] = checkElementByCSSSelector(driver, "nav.vip-container", waitSeconds)
    inDf.loc[idx,'Models'] = checkElementByCSSSelector(driver, "a.model-item-link", waitSeconds)
    inDf.loc[idx,'item'] = inDf.loc[idx,'PriceUnit'] + " " + inDf.loc[idx,'Price']
    
  # WRITE csv
  inDf.to_csv(outCsvPath, index=False)
  return len(inDf.index)


def getFullItems(chromeDriverPath, waitSeconds, newLinksPath, outCsvPath):
  # PROGRAM SETUP
  # Initial Vars
  newLinksDf = pd.read_csv(newLinksPath)

  # Initialize Selenium
  options = webdriver.ChromeOptions()
  # pass in headless argument to options                                                                                                                          
  # options.add_argument('--headless')
  options.add_argument("--disable-infobars")
  # options.add_argument("start-maximized")
  # options.add_argument('--no-sandbox')
  options.add_argument("--disable-extensions")         
  options.add_argument('--disable-dev-shm-usage') 
  driver = webdriver.Chrome(chromeDriverPath, options=options)

  # Selenium scrapper - SLOW
  rows = getItemFields(driver, waitSeconds, newLinksDf, outCsvPath)

  # End Selenium
  driver.quit()
  return rows