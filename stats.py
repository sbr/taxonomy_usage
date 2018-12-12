#!/usr/bin/python
# -*- coding: utf-8 -*-
import subprocess
import sqlite3
import sys
import csv

def writeCSV(fileName, rows):
    with open(fileName, 'wb') as csvfile:
        writer = csv.writer(csvfile, delimiter=',',quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for row in rows:
            writer.writerow(row)

def getStats(c):

    totalElements = subprocess.check_output("grep -r -P \"id=\\\"DE\d+\" " +sys.argv[1]+"/sbr_au_taxonomy/icls -o | cut -d\"\\\"\" -f2 | sort -u | wc -l", shell=True)



    rows = []
    agencies = {}
    for row in c.execute("select distinct substr(report,1,length(report) - 6), agency from usage_de;"):
        data = str(row[0])
        agency = str(row[1])
        rows.append(data)
        if not agency in agencies: agencies[agency] = 0
        agencies[agency] = agencies[agency] + 1
    numReports = len(rows)


    apraPercent = "{0:.2f}".format((agencies["apra"] / float(numReports)) * 100)
    atoPercent = "{0:.2f}".format((agencies["ato"] / float(numReports)) * 100)
    other = 100 - float(apraPercent) - float(atoPercent)

    numReleases = sys.argv[1].split(".")[2]

    print "*",str(numReleases), "production releases of the dictionary since the program went live"
    print "* Current dictionary contains", totalElements,"unique data items"
    print "*", str(numReports), "unique reports in the dictionary"
    print "\t*", apraPercent, "% APRA"
    print "\t*", atoPercent, "% ATO"
    print "\t*", other, "% Other"


if len(sys.argv) != 2:
    print "usage:\n./stats.py 20xx.02.xx.sbr.au/sbr_au"
    sys.exit(1)

taxonomyVersion = sys.argv[1].replace("/sbr_au","")
dbName = taxonomyVersion + ".db"

#print "Using taxonomy version: " + taxonomyVersion
#print "Opening " + dbName

conn = sqlite3.connect(dbName)
c = conn.cursor()
getStats(c)
