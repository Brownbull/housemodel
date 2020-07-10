# -*- coding: utf-8 -*-
"""
  @author: Brownbull - Gabriel Carcamo - carcamo.gabriel@gmail.com
  Program functions module
"""
import time
import inspect
from datetime import datetime

# https://stackoverflow.com/questions/19926089/python-equivalent-of-java-stringbuffer/29000388
def array2Str(array, separator):
    out_str = separator.join(num for num in array)
    return out_str

def getTimeAndStamp():
  nowTime = time.time()
  return nowTime, datetime.fromtimestamp(nowTime)

# https://stackoverflow.com/questions/18425225/getting-the-name-of-a-variable-as-a-string/18425523
def retrieveName(var):
  """
  Gets the name of var. Does it from the out most frame inner-wards.
  :param var: variable to get name from.
  :return: string
  """
  for fi in reversed(inspect.stack()):
    names = [var_name for var_name, var_val in fi.frame.f_locals.items() if var_val is var]
    if len(names) > 0:
      return names[0]
