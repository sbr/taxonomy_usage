#!/usr/bin/python
import subprocess
import sys
import re
import os
import sqlite3

usage = "Usage: ./eeu.py full_path_to_sbr_au"

labelLookup = {}
def getDataElementLabel(path, id):
    if id in labelLookup:
        return labelLookup[id]

    path = path + ".data.xsd"
    newId = '"' + id + "\\\"\""
    cmd = "grep " + newId + " " + path

    for part in subprocess.check_output(cmd, shell=True).replace("\t",' ').replace("  "," ").split(" "):
        if part.startswith("name"):
            name = part.replace("\"","").replace("name=","")
            labelLookup[id] = name
            return name

def getDimensionLabel(path, id):
    if id in labelLookup:
        return labelLookup[id]

    path = path + ".data.xsd"
    newId = '"' + id + "\\\"\""
    cmd = "grep " + newId + " " + path

    for part in subprocess.check_output(cmd, shell=True).replace("\t",' ').replace("  "," ").split(" "):
        if part.startswith("name"):
            name = part.replace("\"","").replace("name=","")
            labelLookup[id] = name
            return name

def getDimensionsInReports(c):
    print "Extracting Dimension usage from", sbr_au

    c.execute("CREATE TABLE usage_dm(filename text, controlledid text, agency text, report text, label text)")
    x = subprocess.check_output("grep -r -i '#DM[0-9]\+' " + sbr_au_reports + " | grep -i private.*deflink", shell=True)
    for line in x.split('\n'):
        if line == "": continue
        dm = Dimension(line)
        c.execute("INSERT INTO usage_dm VALUES ('{0}','{1}','{2}','{3}', '{4}')".format(dm.filename, dm.controlledid, dm.agency, dm.report, dm.label))
    conn.commit()

def getDataElementsInReports(c):
    print "Extracting DataElement usage from", sbr_au
    c.execute("CREATE TABLE usage_de(classification text, controlledid text, agency text, report text, label text)")

    x = subprocess.check_output("grep -r -i '#DE[0-9]\+' " + sbr_au_reports + " | grep -i preslink", shell=True)
    for line in x.split('\n'):
        if line == "": continue
        if line.find("link:roleRef") > -1: continue
        de = DataElement(line)
        c.execute("INSERT INTO usage_de VALUES ('{0}','{1}','{2}','{3}', '{4}')".format(de.classification, de.controlledid, de.agency, de.report, de.label))
    conn.commit()

    agencies = []
    for row in c.execute('SELECT distinct agency FROM usage_de ORDER BY agency'):
        name = str(row[0])
        agencies.append(name)
    print "Here are the agencies in the taxonomy:"

    for name in agencies:
            c.execute("select count(distinct(controlledid)) from usage_de where agency = '{0}' and controlledid not in(select controlledid from usage_de where agency != '{0}')".format(name))
            print "\t" + name + " has " + str(c.fetchone()[0]) + " unique dataelements that are only used by " + name



class Dimension():
    def __init__(self, line):
        self.line = line
        self.filename = ""
        self.label = ""
        self.controlledid = ""
        self.agency = ""
        self.report = ""
        self.extract()

    def extract(self):
        line = self.line.replace("\t",' ').replace("  "," ").replace("\\","/")
        lineparts = line.split(" ")

        urlparts = lineparts[0].replace(sbr_au_reports,"").split('/')
        if(len(urlparts) < 2):
            print "Couldn't extract agency name from " + self.line
            sys.exit(1)

        self.agency = urlparts[1]
        exitIfNull(self.agency, "Couldn't extract agency name from " + line)

        self.report = urlparts[-1][:-len(".defLink.xml:")]
        exitIfNull(self.report, "Couldn't extract report name from " + line)

        for p in range(0 ,len(lineparts)):
            part = lineparts[p]

            if part.startswith("xlink:href="):
                href = part
                dimensionparts = re.sub(r".*icls/", "", part).replace("\"","").split("#")
                self.filename = re.sub(r".*sbr_au_taxonomy/dims/", "", dimensionparts[0])[:-len(".data.xsd")]
                self.controlledid = dimensionparts[1]

            if part.startswith("xlink:label="):
                label = part[(part.find("\"")) + 1 :]
                self.label = label[0: label.rfind("\"")]

        exitIfNull(self.label, "Couldn't extract label from " + line)
        if self.label.find("Dimension") == -1:
            self.label = getDimensionLabel(dims + self.filename, self.controlledid)
        exitIfNull(self.filename, "Couldn't extract filename from " + line)
        exitIfNull(self.controlledid, "Couldn't extract controlledid from " + line)


class DataElement():
    def __init__(self, line):
        self.line = line
        self.classification = ""
        self.label = ""
        self.controlledid = ""
        self.agency = ""
        self.report = ""
        self.extract()

    def __str__(self):
        return self.agency + ", " + self.report + ", " + self.classification + ", " + self.controlledid  + ", " + self.label + "\n"

    def extract(self):
        line = self.line.replace("\t",' ').replace("  "," ").replace("\\","/")
        lineparts = line.split(" ")

        urlparts = lineparts[0].replace(sbr_au_reports,"").split('/')
        if(len(urlparts) < 2):
            print "Couldn't extract agency name from " + self.line
            sys.exit(1)
        self.agency = urlparts[1]
        exitIfNull(self.agency, "Couldn't extract agency name from " + line)

        self.report = urlparts[-1][:-len(".presLink.xml:")]
        exitIfNull(self.report, "Couldn't extract report name from " + line)

        href = ""
        for p in range(0 ,len(lineparts)):
            part = lineparts[p]
            if part.startswith("xlink:href="):
                href = part
                dataelementparts = re.sub(r".*icls/", "", part).replace("\"","").split("#")
                self.classification = dataelementparts[0][:-len(".data.xsd")]
                self.controlledid = dataelementparts[1]

            if part.startswith("xlink:title="):
                label = part[(part.find("\"")) + 1 :]
                self.label = label[0: label.rfind("\"")]


        exitIfNull(self.classification, "Couldn't extract classification from " + line)
        exitIfNull(self.controlledid, "Couldn't extract controlledid from " + line)
        if self.label == "" or self.label.find(".") == -1:
            self.label = getDataElementLabel(icls + self.classification, self.controlledid)
        exitIfNull(self.label, "Couldn't extract label from " + line)

def exitIfNull(value, message):
    if value == "" or len(value) == 0 or value == None:
        print message
        exit(1)

if len(sys.argv) != 2:
    print usage
    sys.exit(1)

sbr_au = sys.argv[1]
if (sbr_au[-1] != '/'): sbr_au = sbr_au + '/'
sbr_au_reports = sbr_au + "sbr_au_reports"
icls = sbr_au + "sbr_au_taxonomy/icls/"
dims = sbr_au + "sbr_au_taxonomy/dims/"

usage_db_filename =  sbr_au.replace("/","_")[:-len("/sbr_au/")]+".db"
if os.path.exists(usage_db_filename):
    print "Removing previous database : " + usage_db_filename
    os.remove(usage_db_filename)
print "Created usage database: '" + usage_db_filename + "'"

conn = sqlite3.connect(usage_db_filename)
c = conn.cursor()

#getDataElementsInReports(c)
getDimensionsInReports(c)

conn.close()
print "done."

# Domain Memebers and Values used in a report
# grep '#D[VM][0-9]\+' ctr.0007.private.02.00.defLink.xml

# Elements that are unique to an agency
# select distinct(controlledid), label from usage where agency = 'apra' and controlledid not in(select controlledid from usage where agency != 'apra') order by label

# Elemens used by an agency that are also used by others
# select distinct(controlledid), label from usage where agency = 'apra' and controlledid in(select controlledid from usage where agency != 'apra') order by label
