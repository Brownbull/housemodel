# -*- coding: utf-8 -*-
"""
  @author: Brownbull - Gabriel Carcamo - carcamo.gabriel@gmail.com
  Program functions module
"""
import re
import time
import inspect
import unicodedata
from datetime import datetime
from dateutil.parser import parse

# https://stackoverflow.com/questions/944700/how-can-i-check-for-nan-values?rq=1
def isNaN(num):
    return num != num

# https://stackoverflow.com/questions/10711116/strip-spaces-tabs-newlines-python
def rmEscSep(string):
  return ' '.join(str(string).split())

# https://stackoverflow.com/questions/44431730/how-to-replace-accented-characters-in-python
def stripAccents(text):
    try:
        text = unicode(text, 'utf-8')
    except NameError: # unicode is a default on python 3 
        pass
    text = unicodedata.normalize('NFD', text)\
           .encode('ascii', 'ignore')\
           .decode("utf-8")
    return str(text)

def sanitize(string, rmve=","):
  return stripAccents(rmEscSep(string).replace(rmve,"").lower())

# https://stackoverflow.com/questions/19926089/python-equivalent-of-java-stringbuffer/29000388
def array2Str(array, separator):
  out_str = separator.join(str(num) for num in array)
  # out_str = ""
  # for e in array:
  #   out_str = out_str + separator + e
  return out_str

# https://stackoverflow.com/questions/3271478/check-list-of-words-in-another-string
def strOfListInPhrase(strList, Phrase):
  return any(word in Phrase for word in strList)
  
def strListInPhrase(strList, Phrase):
  return all(word in Phrase for word in strList)

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

def getTimeAndStamp():
  nowTime = time.time()
  return nowTime, datetime.fromtimestamp(nowTime)

def ifDateSave(string, default, format="%d/%m/%Y"):
  if string is not None and not isNaN(string):
    if isDate(string):
      return strToDate(string, format)
    else:
      return default
  else:
    return None

# https://stackoverflow.com/questions/25341945/check-if-string-has-date-any-format
def isDate(string, fuzzy=False):
    """
    Return whether the string can be interpreted as a date.

    :param string: str, string to check for date
    :param fuzzy: bool, ignore unknown tokens in string if True
    """
    try: 
        parse(string, fuzzy=fuzzy)
        return True

    except ValueError:
        return False

def strToDate(string, format="%Y/%m/%d", fuzzy=False):
  return parse(string, fuzzy=fuzzy).strftime(format)

# https://stackoverflow.com/questions/4703390/how-to-extract-a-floating-number-from-a-string
def getNumbrs(string):
  if string is not None and not isNaN(string):
    # return [int(s) for s in string.split() if s.isdigit()]
    return re.findall(r"[-+]?\d*\.\d+|\d+", str(string))
  else:
    return []