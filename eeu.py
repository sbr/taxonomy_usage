#!/usr/bin/python
import subprocess
import sys
import re
import os
import sqlite3

usage = "Usage: ./eeu.py full_path_to_sbr_au"

class DataElement():
    def __init__(self, line):
        self.line = line
        self.classification = ""
        self.label = ""
        self.controlledid = ""
        self.agency = ""
        self.report = ""
        self.extractDataelement()

    def __str__(self):
        return self.agency + ", " + self.report + ", " + self.classification + ", " + self.controlledid + "\n"# + ", " + self.label

    def extractDataelement(self):
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

        for p in range(0 ,len(lineparts)):
            part = lineparts[p]
            if part.startswith("xlink:href="):
                dataelementparts = re.sub(r".*icls/", "", part).replace("\"","").split("#")
                self.classification = dataelementparts[0][:-len(".data.xsd")]
                self.controlledid = dataelementparts[1]

            #if part.startswith("xlink:title="):
            #    label = part[(part.find("\"")) + 1 :]
            #    self.label = label[0: label.rfind("\"")]

        exitIfNull(self.classification, "Couldn't extract classification from " + line)
        exitIfNull(self.controlledid, "Couldn't extract controlledid from " + line)
        #exitIfNull(self.label, "Couldn't extract label from " + line)


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

print "Extracting DataElement usage from", sbr_au

de_usage_filename =  sbr_au.replace("/","_")[:-len("/sbr_au/")]+".db"

if os.path.exists(de_usage_filename):
    print "Removing previous database : " + de_usage_filename
    os.remove(de_usage_filename)

conn = sqlite3.connect(de_usage_filename)
c = conn.cursor()

c.execute("CREATE TABLE usage(classification text, controlledid text, agency text, report text)")

print "Finding out what elements are used in reports..."

x = subprocess.check_output("grep -r -i '#DE[0-9]\+' " + sbr_au_reports + " | grep -i preslink", shell=True)
for line in x.split('\n'):
    if line == "": continue
    de = DataElement(line)
    c.execute("INSERT INTO usage VALUES ('{0}','{1}','{2}','{3}')".format(de.classification, de.controlledid, de.agency, de.report))
conn.commit()

print "Created data element in report usage database: '" + de_usage_filename + "'"

agencies = []
for row in c.execute('SELECT distinct agency FROM usage ORDER BY agency'):
    name = str(row[0])
    agencies.append(name)
print "Here are the agencies in the taxonomy:"

for name in agencies:
        c.execute("select count(distinct(controlledid)) from usage where agency = '{0}' and controlledid not in(select controlledid from usage where agency != '{0}')".format(name))
        print "\t" + name + " has " + str(c.fetchone()[0]) + " unique dataelements that are only used by " + name

conn.close()
print "done."


# Number of times a de is used by an agency
# select controlledid, count((controlledid)), agency from usage group by controlledid
