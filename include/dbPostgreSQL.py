# -*- coding: utf-8 -*-
"""
  @author: Brownbull - Gabriel Carcamo - carcamo.gabriel@gmail.com
  PostgreSQL database functions
"""
import psycopg2
import getpass 
from include.logs import *

def dbGetConn(log, db, user, hst, prt):
  # passwd = input("Postgresql connect to {} {} {} {}:\n".format(db, user, hst, prt))
  passwd = getpass.getpass(prompt="Postgresql connect to {} {} {} {}:\n".format(db, user, hst, prt)) 
  try:
    dbConn = psycopg2.connect(database = db, user = user, password = passwd, host = hst, port = prt)
  except:
    logPrint(log, "I am unable to connect to the database") 
    exit()
  logPrint(log, "Connection Open OK")
  return dbConn

def dbEnd(log, dbConn):
  dbConn.close()
  logPrint(log, "Connection Closed OK")

def dbCheckTableExists(dbConn, table):
  dbcur = dbConn.cursor()
  dbcur.execute("""
    SELECT COUNT(*)
    FROM information_schema.tables
    WHERE table_name = '{0}'
    """.format(table.replace('\'', '\'\'')))
  if dbcur.fetchone()[0] == 1:
    dbcur.close()
    return True
  dbcur.close()
  return False

def dbCheckViewExists(dbConn, view):
  return dbCheckTableExists(dbConn, view)

def dbExec(log, dbConn, qry):
  dbCursor = dbConn.cursor()
  try:
    dbCursor.execute(qry)
  except psycopg2.Error as e:
    logPrint(log, "dbExec Failed:")
    logPrint(log, e.pgerror)
    logPrint(log, e.diag.message_detail)
  except psycopg2.OperationalError as e:
    logPrint(log, "dbExec Failed:")
    logPrint(log, e.pgerror)
    logPrint(log, e.diag.message_detail)
  except:
    logPrint(log, "Error on Execution of: {}".format(qry))
    exit()
  dbConn.commit()
  logPrint(log, "Execution OK.")

def dbExecFile(log, dbConn, file):
  logPrint(log, "Executing from sql script: {}".format(file))
  with open(file, 'r', encoding="utf-8") as qryFile:
    return dbExec(log, dbConn, qryFile.read())

# https://stackoverflow.com/questions/22776849/how-to-save-results-of-postgresql-to-csv-excel-file-using-psycopg2
def dbExecFileToCSV(log, dbConn, file, outCsvPath):
  with open(file, 'r', encoding="utf-8") as qryFile:
    qry = qryFile.read()
    csvQry = "copy ({0}) to stdout with csv header".format(qry)
    dbCursor = dbConn.cursor()
    try:
      with open(outCsvPath, 'w') as f:
        dbCursor.copy_expert(csvQry, f)
    except psycopg2.Error as e:
      logPrint(log, "dbExec Failed:")
      logPrint(log, e.pgerror)
      logPrint(log, e.diag.message_detail)
    except psycopg2.OperationalError as e:
      logPrint(log, "dbExec Failed:")
      logPrint(log, e.pgerror)
      logPrint(log, e.diag.message_detail)
    except:
      logPrint(log, "Error on Execution of: {}".format(qry))
      exit()
    dbConn.commit()
    logPrint(log, "Execution OK.")

