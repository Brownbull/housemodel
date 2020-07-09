# -*- coding: utf-8 -*-
"""
  @author: Brownbull - Gabriel Carcamo - carcamo.gabriel@gmail.com
  ETL - Extract Transform Load Module
"""
# IMPORT LIBRARIES
from main.ETL.imports import *
from include.logs import *
from include.files import *

# CHECK ARGUMENTS
parser = argparse.ArgumentParser(description='Main process of Transform Module.')
parser.add_argument('-tConfig','-tc', '-c', help='Tranform Config File Path', default="config/A02_T_config.yaml")
parser.add_argument('-sample','-sm', '-sp', required=False, help='Numbers of records to take as sample', default=0)
parser.add_argument('-info','-i', action='store_const', const=True, default=True, help='Dataframes Information Flag.')
parser.add_argument('-force','-f', action='store_const', const=True, default=False, help='Force Flag, delete any output file in place.')
parser.add_argument('-stats','-s', action='store_const', const=True, default=True, help='Stats Flag')
parser.add_argument('-debug','-d', action='store_const', const=True, default=True, help='Debug Flag')
args = parser.parse_args()

if args.debug:
  print("# Imported Libraries:") 
  getVersions()
  print("# Debug Options:\n{}". format(args))

logPath = initFileInPath("logs", "log")

logPrint(logPath,"asd")

print ("\neof ETL.py")