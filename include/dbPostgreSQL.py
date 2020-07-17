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

def dbExec(log, dbConn, qry):
  dbCursor = dbConn.cursor()
  try:
    dbCursor.execute(qry)
  except:
    logPrint(log, "Error on Execution of: {}".format(qry))
    exit()
  dbConn.commit()

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
