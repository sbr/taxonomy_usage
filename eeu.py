#!/usr/bin/python
# -*- coding: utf-8 -*-
import subprocess
import sys
import re
import os
import sqlite3
from bs4 import BeautifulSoup

usage = "Usage: ./eeu.py full_path_to_sbr_au"

labelLookup = {}
datatypeLookup = {}
reclassificaionLookup = {
"DE13194":"baf/bafpo/bafpo1",
"DE2056":"baf/bafpo/bafpo1",
"DE2591":"baf/bafpr/bafpr1",
"DE3535":"baf/bafpr/bafpr1",
"DE5225":"baf/bafpo/bafpo4",
"DE8583":"baf/bafpr/bafpr1",
"DE9":"py/pyid/pyid",
"DE9087":"baf/bafot/bafot",
"DE9088":"baf/bafot/bafot",
"DE9089":"baf/bafot/bafot"}

def getDataElementLabelsFromLabLink(path, elements):
    path = icls + path + ".labLink.xml"
    #print "Getting labels from",path
    f = open(path)
    soup = BeautifulSoup(f, 'xml')
    labels = soup.findAll("link:label")
    for label in labels:
        controlledid = label['xlink:label'].replace("lbl_","")
        if (controlledid not in elements): continue
        #print controlledid, label['xlink:role'], label.text
        role = "label"
        if (label['xlink:role'].lower().find("definition") > 0):role = "definition"
        if (label['xlink:role'].lower().find("guidance") > 0):role = "guidance"

        #label.text = label.text.replace("â€™","''")
        #label.text.decode("utf-8").replace("â€™", "''").encode("utf-8")
        labelText = label.text.encode('utf-8').replace("â€™","'").replace("â€˜","'").decode("utf-8")

        try:
            labelAsString = str(labelText)
        except:
            print "There is an invalid encoding in label for dataelement",controlledid
            print label.text
            sys.exit(1)

        c.execute("INSERT INTO labels VALUES ( ?, ?, ? )", (controlledid, role, labelText))

def getLabelsForDataElements(c):
    print "Getting labels for DataElements"
    c.execute("DROP TABLE IF EXISTS labels")
    c.execute("CREATE TABLE labels(controlledid text, labelrole text, label text)")

    fileList = []
    for row in c.execute("select distinct classification from latest_de"):
        fileList.append(str(row[0]))

    for file in fileList:
        deList = []
        for row in c.execute("select controlledid from latest_de where classification = '{0}'".format(file)):
            deList.append(str(row[0]))
        getDataElementLabelsFromLabLink(file,deList)

def camel_case_split(identifier):
    print identifier
    identifier = identifier.replace(".","")
    matches = re.finditer('.+?(?:(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])|$)', identifier)
    return " ".join([m.group(0) for m in matches])

def loadDataElementDetails(path, id):
    # 	<xsd:element name="OrganisationNameDetails.OrganisationalName.Text" substitutionGroup="xbrli:item" nillable="true" id="DE55" xbrli:periodType="duration" type="dtyp.02.00:sbrOrganisationNameItemType" block="substitution"/>
    if id in labelLookup:
        return labelLookup[id]

    path = path + ".data.xsd"
    newId = '"' + id + "\\\"\""
    cmd = "grep " + newId + " " + path

    for part in subprocess.check_output(cmd, shell=True).replace("\t",' ').replace("  "," ").split(" "):
        if part.startswith("name"):
            name = part.replace("\"","").replace("name=","")
            labelLookup[id] = name
        if part.startswith("type"):
            datatype = part.replace("\"","").replace("type=","")
            datatypeLookup[id] = datatype


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



def populateDataelementLatestVersion(de):
    c.execute("select classification from latest_de where controlledid = '{0}'".format(de.controlledid))
    existingClassification = c.fetchone()
    if(existingClassification == None):
        if(de.controlledid in reclassificaionLookup): de.classification = reclassificaionLookup[de.controlledid] + ".02.00"
        c.execute("INSERT INTO latest_de VALUES ('{0}','{1}','{2}','{3}')".format(de.classification, de.controlledid,de.label, de.datatype))
        return

    existingClassification = existingClassification[0]

    if(existingClassification == de.classification): return

    sameICLS = existingClassification[:-2] == de.classification[:-2]

    if sameICLS and (existingClassification < de.classification):
        #print "Updating version of ", de.controlledid, "from", existingClassification,"to",de.classification
        c.execute("delete from latest_de where controlledid = '{0}'".format(de.controlledid))
        c.execute("INSERT INTO latest_de VALUES ('{0}','{1}','{2}','{3}')".format(de.classification, de.controlledid, de.label, de.datatype))
        return

    if not sameICLS and de.classification[:-6] != reclassificaionLookup[de.controlledid]:

        if(de.controlledid not in reclassificaionLookup):
            print "Not sure what to do about reclasification that isn't in map!", de.controlledid, existingClassification, de.classification
            sys.exit(1)

        #print "reclassificaionLookup says",de.controlledid,"was reclassified to",reclassificaionLookup[de.controlledid]
        c.execute("delete from latest_de where controlledid = '{0}'".format(de.controlledid))
        c.execute("INSERT INTO latest_de VALUES ('{0}','{1}','{2}','{3}')".format(de.classification, de.controlledid, de.label, de.datatype))


def getDataElementsInReports(c):
    print "Extracting DataElement usage from", sbr_au
    c.execute("CREATE TABLE usage_de(classification text, controlledid text, agency text, report text, label text, datatype text)")
    c.execute("CREATE TABLE latest_de(classification text, controlledid text, label text, datatype text)")


    x = subprocess.check_output("grep -r -i '#DE[0-9]\+' " + sbr_au_reports + " | grep -i preslink", shell=True)
    for line in x.split('\n'):
        if line == "": continue
        if line.find("link:roleRef") > -1: continue
        de = DataElement(line)
        c.execute("INSERT INTO usage_de VALUES ('{0}','{1}','{2}','{3}', '{4}', '{5}')".format(de.classification, de.controlledid, de.agency, de.report, de.label, de.datatype))
        populateDataelementLatestVersion(de)
    conn.commit()


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
        self.datatype = ""
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
        loadDataElementDetails(icls + self.classification, self.controlledid)
        self.datatype = datatypeLookup[self.controlledid]
        exitIfNull(self.datatype, "Couldn't extract datatype from " + line)

        if self.label == "" or self.label.find(".") == -1:
            self.label = labelLookup[self.controlledid]

        exitIfNull(self.label, "Couldn't extract label from " + line)


def makeExampleTable(c):
    dataElements = []
    for row in c.execute("select controlledid from latest_de limit 5"):
        dataElements.append(str(row[0]))

    for dataElement in dataElements:
        c.execute("select label from labels where controlledid = '{0}' and labelrole = 'label'".format(dataElement))
        label = c.fetchone()[0]
        c.execute("select label from labels where controlledid = '{0}' and labelrole = 'definition'".format(dataElement))
        definition = c.fetchone()[0]

        print """
        <tr>
            <td class="table_name"><strong><a href="#">{0}</a></strong></td>
            <td class="table_definition">{1}</td>
            <td class="table_domain"><a href="#">Standard Business Reporting</a></td>
            <td class="table_status">Preferred Standard</td>
        </tr>
        """.format(label, definition)


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

getDataElementsInReports(c)
getLabelsForDataElements(c)
#getDimensionsInReports(c)

#makeExampleTable(c)

conn.commit()
conn.close()
print "done."

# Domain Memebers and Values used in a report
# grep '#D[VM][0-9]\+' ctr.0007.private.02.00.defLink.xml

# Elements that are unique to an agency
# select distinct(controlledid), label from usage where agency = 'apra' and controlledid not in(select controlledid from usage where agency != 'apra') order by label

# Elemens used by an agency that are also used by others
# select distinct(controlledid), label from usage where agency = 'apra' and controlledid in(select controlledid from usage where agency != 'apra') order by label


# Datatypes used only by this agency
# select distinct(controlledid), label, datatype from usage_de where agency = 'apra' and controlledid not in(select controlledid from usage_de where agency != 'apra') and datatype not like 'xbrli%' and datatype not in (select datatype from usage_de where agency !='apra') order by label
