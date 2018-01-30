#!/usr/bin/python
# -*- coding: utf-8 -*-
import sqlite3
import sys
import csv

def writeCSV(fileName, rows):
    with open(fileName, 'wb') as csvfile:
        writer = csv.writer(csvfile, delimiter=',',quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for row in rows:
            writer.writerow(row)

def getElementsUniqueToAPRA(c):
    rows = []
    for row in c.execute("select distinct(usage_de.controlledid), latest_de.label from usage_de, latest_de where latest_de.controlledid = usage_de.controlledid and agency = 'apra' and usage_de.controlledid not in(select controlledid from usage_de where agency != 'apra') order by usage_de.controlledid"):
        rows.append([str(row[0]),str(row[1])])
    return rows

def getElementsAPRAShare(c):
    rows = []
    for row in c.execute("select distinct(usage_de.controlledid), latest_de.label from usage_de, latest_de where latest_de.controlledid = usage_de.controlledid and agency = 'apra' and usage_de.controlledid in(select controlledid from usage_de where agency != 'apra') order by usage_de.controlledid"):
        rows.append([str(row[0]),str(row[1])])
    return rows

def getDataypesUniqueToAPRA(c):
    rows = []
    for row in c.execute("select * from latest_de where datatype not like 'xbrli%' and controlledid in (select distinct(controlledid) from usage_de where agency = 'apra' and controlledid not in(select controlledid from usage_de where agency != 'apra'))"):
        rows.append([str(row[1]),str(row[2]),str(row[3])])
    return rows

def getDataypesAPRAShare(c):
    rows = []
    for row in c.execute("select * from latest_de where datatype not like 'xbrli%' and controlledid in (select distinct(controlledid) from usage_de where agency = 'apra' and controlledid in(select controlledid from usage_de where agency != 'apra'))"):
        rows.append([str(row[1]),str(row[2]),str(row[3])])
    return rows

def getDimensionsUniqueToAPRA(c):
    rows = []
    for row in c.execute("select distinct(controlledid), label from usage_dm where agency = 'apra' and controlledid not in(select controlledid from usage_dm where agency != 'apra') order by controlledid"):
        rows.append([str(row[0]),str(row[1])])
    return rows

def getDimensionsAPRAShare(c):
    rows = []
    for row in c.execute("select distinct(controlledid), label from usage_dm where agency = 'apra' and controlledid in(select controlledid from usage_dm where agency != 'apra') order by controlledid"):
        rows.append([str(row[0]),str(row[1])])
    return rows



if len(sys.argv) != 2:
    print "usage:\n./apra_usage.py 20xx.02.xx.sbr.au/sbr_au"
    sys.exit(1)

taxonomyVersion = sys.argv[1].replace("/sbr_au","")
dbName = taxonomyVersion + ".db"

print "Using taxonomy version: " + taxonomyVersion
print "Opening " + dbName

conn = sqlite3.connect(dbName)
c = conn.cursor()

writeCSV(taxonomyVersion + " - Elements unique to APRA.csv", getElementsUniqueToAPRA(c))
writeCSV(taxonomyVersion + " - Elements APRA Share.csv", getElementsAPRAShare(c))
writeCSV(taxonomyVersion + " - Datatypes unique to APRA.csv", getDataypesUniqueToAPRA(c))
writeCSV(taxonomyVersion + " - Datatypes APRA Share.csv", getDataypesAPRAShare(c))
writeCSV(taxonomyVersion + " - Dimensions unique to APRA.csv", getDimensionsUniqueToAPRA(c))
writeCSV(taxonomyVersion + " - Dimensions APRA Share.csv", getDimensionsAPRAShare(c))
