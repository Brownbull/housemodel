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
# Multithread
import os
import time
from multiprocessing import Process
from multiprocessing import Pool

# https://stackoverflow.com/questions/49123439/python-how-to-run-process-in-detached-mode
def detachify(func):
  """Decorate a function so that its calls are async in a detached process.

  Usage
  -----

  .. code::
          import time

          @detachify
          def f(message):
              time.sleep(5)
              print(message)

          f('Async and detached!!!')

  """
  # create a process fork and run the function
  def forkify(*args, **kwargs):
    if os.fork() != 0:
      return
    func(*args, **kwargs)

  # wrapper to run the forkified function
  def wrapper(*args, **kwargs):
    proc = Process(target=lambda: forkify(*args, **kwargs))
    proc.start()
    proc.join()
    return

  return wrapper

def checkElementByCSSSelector(driver, cssSel, waitSeconds):
  try:
    element = WebDriverWait(driver, waitSeconds).until(
      EC.presence_of_element_located((By.CSS_SELECTOR, cssSel))
    )
    # element = driver.find_element_by_css_selector(cssSel)
    return element.text
  except:
    return None

# @detachify
def getItemFields(driver, waitSeconds, inDf, outCsvPath):
  for idx, row in inDf.iterrows():
    # Open
    driver.get(row['link'])
    # Get Fields
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
    
  # WRITE csv
  inDf.to_csv(outCsvPath, index=False)


# PROGRAM SETUP
chromeDriverPath = "D:\Reference\python\WebScrapping\chromeDriver\chromedriver84.exe"
dfPath = "D:\Reference\housemodel\data\ETL\E\python\houses.csv"
partPath= "D:\Reference\housemodel\data\ETL\E\python\housesPart_"
outCsvPath = "D:\Reference\housemodel\data\ETL\E\python\housesFull_"
waitSeconds = 0.01
# Initial Vars
inDf = pd.read_csv(dfPath)
lenDf = len(inDf)
partitions = 10
partitionSize = math.floor(lenDf/partitions)
for i in range(0,10):
  # print("ini: {}".format(int((partitionSize * i) + 1)))
  # print("end: {}".format(int(lenDf - (partitionSize * (partitions - i - 1)))))
  inDf.loc[int((partitionSize * i) + 1):int(lenDf - (partitionSize * (partitions - i - 1))), :].to_csv(partPath + str(i) + ".csv")
# Initialize
options = webdriver.ChromeOptions()
# pass in headless argument to options                                                                                                                          
options.add_argument('--headless')
options.add_argument("--disable-infobars")
options.add_argument("start-maximized")
options.add_argument('--no-sandbox')
options.add_argument("--disable-extensions")         
options.add_argument('--disable-dev-shm-usage') 
driver = webdriver.Chrome(chromeDriverPath, options=options)

# parmsArr = []
for i in range(0,10):
  inDf = pd.read_csv(partPath + str(i) + ".csv")
  outCsv = outCsvPath + str(i) + ".csv"
  # parmsArr.append((driver, waitSeconds, inDf, outCsv))
  # getItemFields(driver, waitSeconds, inDf, outCsv)
  p = Process(target=getItemFields(driver, waitSeconds, inDf, outCsv))
  # p.daemon = True 
  p.start()
  p.join
# with Pool(5) as p:
#   print(p.map(getItemFields, parmsArr))
# Exec
# getItemFields(driver, waitSeconds, inDf, outCsvPath)
# End
driver.quit()