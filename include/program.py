# -*- coding: utf-8 -*-
"""
  @author: Brownbull - Gabriel Carcamo - carcamo.gabriel@gmail.com
  Program functions module
"""
import inspect

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
