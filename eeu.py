#!/usr/bin/python
# -*- coding: utf-8 -*-
import subprocess
import sys
import re
import os
import sqlite3
from bs4 import BeautifulSoup
import json

usage = "Usage: ./eeu.py full_path_to_sbr_au"

labelLookup = {}
datatypeLookup = {}
datatypeJSONLookup = {}
xbrlPartsLookup = {}
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
"DE9089":"baf/bafot/bafot",
"DE260":"baf/bafpo/bafpo4"
}

agencyLookup = {
"apra":"Australian Prudential Regulation Agency",
"asic": "Australian Securities and Investments Commission",
"ato": "Australian Taxation Office",
"osract": "ACT Office of Revenue",
"osrnsw": "NSW Office of Revenue",
"osrnt": "NT Office of Revenue",
"osrqld": "QLD Office of Revenue",
"osrsa": "SA Office of Revenue",
"osrtas": "TAS Office of Revenue",
"osrvic": "VIC Office of Revenue",
"osrwa": "WA Office of Revenue",
"sprstrm": "Super Stream"
}

xbrlDataTypeMap = {
"xbrli:stringItemType": "string",
"xbrli:tokenItemType": "string",
"xbrli:decimalItemType": "float",
"xbrli:monetaryItemType": "float",
"xbrli:booleanItemType": "boolean",
"xbrli:dateItemType": "string",
"xbrli:nonNegativeIntegerItemType": "int",
"xbrli:positiveIntegerItemType": "int",
"xbrli:pureItemType": "float",
"xbrli:gDayItemType": "string",
"xbrli:gMonthItemType": "string",
"xbrli:gYearItemType": "string",
"xbrli:fractionItemType": "float",
"xbrli:dateTimeItemType": "string",
"xbrli:integerItemType": "int",
"xbrli:sharesItemType": "float",
"xbrli:floatItemType": "float",
"xbrli:timeItemType": "string"
}

ignoredDataTypes = ["sbrReportTypeVariationCodeItemType"]

newElementLabels = {
'DE3569': "Organisation Details Activity Event Code",
'DE13250': "Account Open Or Closed"
}

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
        if (label['xlink:role'].lower().find("status") > 0): continue
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

        if(role == "label" and controlledid in newElementLabels): labelText = newElementLabels[controlledid]
        if(role == "label" and str(labelText).strip().count(' ') < 1): exitIfNull("","Found element label with no spaces " + controlledid + " " + labelText)
        if(str(labelText).strip().count(' ') > 1):
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

def loadDataElementDetails(classification, id):
    # 	<xsd:element name="OrganisationNameDetails.OrganisationalName.Text" substitutionGroup="xbrli:item" nillable="true" id="DE55" xbrli:periodType="duration" type="dtyp.02.00:sbrOrganisationNameItemType" block="substitution"/>
    if id in labelLookup:
        return labelLookup[id]

    path = icls + classification + ".data.xsd"
    newId = '"' + id + "\\\"\""
    cmd = "grep " + newId + " " + path
    xbrlParts = {"classification":classification}

    for part in subprocess.check_output(cmd, shell=True).replace("\t",' ').replace("  "," ").split(" "):
        if part.startswith("name"):
            name = part.replace("\"","").replace("name=","")
            xbrlParts["name"] = name
            labelLookup[id] = name
        if part.startswith("type"):
            datatype = part.replace("\"","").replace("type=","")
            datatypeLookup[id] = datatype
        if part.startswith("xbrli:periodType"):
            xbrlParts["period"] = part.replace("xbrli:periodType=\"","").lower().split("\"")[0]
            if(xbrlParts["period"] not in ["instant","duration"]): exitIfNull("","Period wasn't instant or duration: " + str(xbrlParts))
        if part.startswith("xbrli:balance"):
            xbrlParts["balance"] = part.replace("xbrli:balance=\"","").lower().split("\"")[0]
            if(xbrlParts["balance"] not in ["credit","debit"]): exitIfNull("","Balance wasn't credit or debit: " + str(xbrlParts))
    if(xbrlParts != {}): xbrlPartsLookup[id] = xbrlParts

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

    lines = []
    x = subprocess.check_output("grep -r -i '#DE[0-9]\+' " + sbr_au_reports + " | grep -i preslink", shell=True)
    for line in x.split('\n'):
        lines.append(line)

    count = 0
    for line in lines:
        if line == "": continue
        if line.find("link:roleRef") > -1: continue
        de = DataElement(line)
        c.execute("INSERT INTO usage_de VALUES ('{0}','{1}','{2}','{3}', '{4}', '{5}')".format(de.classification, de.controlledid, de.agency, de.report, de.label, de.datatype))
        populateDataelementLatestVersion(de)

        count = count + 1
        if(count % 10000 == 0):
            print "Extracting DataElement usage: [" + str(count) + " of " + str(len(lines)) + "]"
    print "Extracting DataElement usage: [" + str(len(lines)) + " of " + str(len(lines)) + "]\ndone."
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


class DataType():
    def __init__(self, element):
        self.element = element
        self.name = ""
        self.values = []
        self.facets = {}
        self.ignore = False
        self.base = ""
        self.extract()

    def extract(self):
        self.name = self.element['name']
        if(self.name in ignoredDataTypes):
            self.ignore = True
            return
        exitIfNull(self.name, "Couldn't get name from:\n" + str(self.element))

        for part in self.element.descendants:
            if part.name == "enumeration":
                self.values.append(part["value"])

            if part.name in ["maxLength","minLength","minInclusive","pattern","totalDigits","fractionDigits"]:
                self.facets[part.name] = str(part["value"])

            if part.name == "restriction":
                base = part["base"]
                if base in xbrlDataTypeMap:
                    self.base = xbrlDataTypeMap[base]

        #if self.values != []: print "Got enumerations: ", ','.join(self.values)
        #if self.facets != {}: print "Got faets ", str(self.facets)
        #if self.values == [] and self.facets == {}: exitIfNull("", "Got no enumerations or facets from:\n" + str(self.element))
        exitIfNull(self.base, "Couldn't get base type from:\n" + str(self.element))

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
        loadDataElementDetails(self.classification, self.controlledid)
        self.datatype = datatypeLookup[self.controlledid]
        exitIfNull(self.datatype, "Couldn't extract datatype from " + line)

        if self.label == "" or self.label.find(".") == -1:
            self.label = labelLookup[self.controlledid]

        exitIfNull(self.label, "Couldn't extract label from " + line)


def generateOutputJSON(c):
    print "Writing json definitions"
    dataElements = []
    for row in c.execute("select controlledid from latest_de order by controlledid"):
        dataElements.append(str(row[0]))

    sbr = []
    fs = []
    ss = []
    syntax = []
    classifications = []
    intersectionCounts = {}

    count = 0
    for dataElement in dataElements:
        element = {}

        c.execute("select label from labels where controlledid = '{0}' and labelrole = 'label'".format(dataElement))
        element["name"] = c.fetchone()[0]

        try:
            c.execute("select label from labels where controlledid = '{0}' and labelrole = 'definition'".format(dataElement))
            element["definition"] = c.fetchone()[0]
        except:
            pass
        try:
            c.execute("select label from labels where controlledid = '{0}' and labelrole = 'guidance'".format(dataElement))
            element["guidance"] = c.fetchone()[0]
        except:
            pass
        element["status"] = "Standard"

        usage = []
        for agency in c.execute("select distinct agency from usage_de where controlledid = '{0}' order by agency".format(dataElement)):
            usage.append(agencyLookup[agency[0]])
        element["usage"] = usage
        if(not intersectionCounts.has_key(str(usage))): intersectionCounts[str(usage)] = 0
        intersectionCounts[str(usage)] = intersectionCounts[str(usage)] + 1

        justAPRA = (usage == ["Australian Prudential Regulation Agency"])
        justSuper = (usage == ["Super Stream"])
        #ifother = (not(justAPRA and justSuper))
        if justAPRA:
            element["domain"] = "Financial Statistics"
            element["identifier"] = "http://api.gov.au/definition/fs/" + dataElement.lower()
        elif justSuper:
            element["domain"] = "Super Stream"
            element["identifier"] = "http://api.gov.au/definition/ss/" + dataElement.lower()
        else:
            element["domain"] = "Taxation and revenue collection"
            element["identifier"] = "http://api.gov.au/definition/trc/" + dataElement.lower()

        c.execute("select datatype from latest_de where controlledid = '{0}'".format(dataElement))
        datatype = c.fetchone()[0]


        if(datatype.startswith("dtyp")):
            datatype = datatype.split(":")[1]

            dt = datatypeJSONLookup[datatype]

            typeDict = {"type" : dt.base}
            if(dt.values != []): typeDict["values"] = dt.values
            if(dt.facets != {}): typeDict["facets"] = dt.facets
            element["datatype"] = typeDict
        else:
            element["datatype"] = {"type" : xbrlDataTypeMap[datatype]}

        if(element["domain"] == "Taxation and revenue collection"): sbr.append(element)
        if(element["domain"] == "Financial Statistics"): fs.append(element)
        if(element["domain"] == "Super Stream"): ss.append(element)

        if(dataElement in xbrlPartsLookup):
            syn = {"identifier" : element["identifier"],"syntax":{}}
            syn["syntax"]["xbrl"] = xbrlPartsLookup[dataElement]
            syntax.append(syn)

        count = count + 1
        if(count % 100 == 0):
            print "Writing json definitions: [" + str(count) + " of " + str(len(dataElements)) + "]"
    print "Writing json definitions: [" + str(len(dataElements)) + " of " + str(len(dataElements)) + "]\ndone."
    dataElements = None

    print "Here's a breakdown of which elements are used by which agency:"
    for key, value in intersectionCounts.iteritems():
        print key, value

    definitions_file_name = 'trc.json'
    if os.path.exists(definitions_file_name):
        #print "Removing previous", definitions_file_name
        os.remove(definitions_file_name)
    print "Created",definitions_file_name

    text_file = open(definitions_file_name, "w")
    sbr_wrapper = {"domain":"Taxation and revenue collection","acronym":"trc","version":sbr_au_version,"content":sbr}
    text_file.write(json.dumps(sbr_wrapper, sort_keys=True, indent=4, separators=(',', ': ')))
    text_file.close()
    sbr = None
    sbr_wrapper = None

    definitions_file_name = 'fs.json'
    if os.path.exists(definitions_file_name):
        #print "Removing previous", definitions_file_name
        os.remove(definitions_file_name)
    print "Created",definitions_file_name

    text_file = open(definitions_file_name, "w")
    fs_wrapper = {"domain":"Financial Statistics","acronym":"fs","version":sbr_au_version,"content":fs}
    text_file.write(json.dumps(fs_wrapper, sort_keys=True, indent=4, separators=(',', ': ')))
    text_file.close()
    fs = None
    fs_wrapper = None

    definitions_file_name = 'ss.json'
    if os.path.exists(definitions_file_name):
        #print "Removing previous", definitions_file_name
        os.remove(definitions_file_name)
    print "Created",definitions_file_name

    text_file = open(definitions_file_name, "w")
    ss_wrapper = {"domain":"Super Stream","acronym":"ss","version":sbr_au_version,"content":ss}
    text_file.write(json.dumps(ss_wrapper, sort_keys=True, indent=4, separators=(',', ': ')))
    text_file.close()
    ss = None
    ss_wrapper = None


    syntax_file_name = 'syntaxes.json'
    print "Writing syntax to '" + syntax_file_name + "'"
    if os.path.exists(syntax_file_name):
        #print "Removing previous", syntax_file_name
        os.remove(syntax_file_name)
    print "Created",syntax_file_name

    text_file = open(syntax_file_name, "w")
    text_file.write(json.dumps(syntax, sort_keys=True, indent=4, separators=(',', ': ')))
    text_file.close()
    syntax = None


def getDataTypes(c):
    ## The latest datatype file should be enough
    path = fdtn + "dtyp*"
    cmd = "ls "+ path +" | sort"
    paths = subprocess.check_output(cmd, shell=True).split("\n")
    paths.remove('')
    print "Getting datatypes"
    for path in paths:
        print "Datatype file: " + path
        f = open(path)
        soup = BeautifulSoup(f, 'xml')
        types = soup.findAll("xsd:complexType")
        for type in types:
            dt = DataType(type)
            datatypeJSONLookup[dt.name] = dt


def exitIfNull(value, message):
    if value == "" or len(value) == 0 or value == None:
        print message
        exit(1)

if len(sys.argv) != 2:
    print usage
    sys.exit(1)

sbr_au = sys.argv[1]
sbr_au_version = sbr_au[0:10]

if (sbr_au[-1] != '/'): sbr_au = sbr_au + '/'
sbr_au_reports = sbr_au + "sbr_au_reports"
icls = sbr_au + "sbr_au_taxonomy/icls/"
fdtn = sbr_au + "sbr_au_taxonomy/fdtn/"
dims = sbr_au + "sbr_au_taxonomy/dims/"

usage_db_filename =  sbr_au.replace("/","_")[:-len("/sbr_au/")]+".db"

if os.path.exists(usage_db_filename):
    print "Removing previous database : " + usage_db_filename
    os.remove(usage_db_filename)
print "Created usage database: '" + usage_db_filename + "'"

conn = sqlite3.connect(usage_db_filename)
c = conn.cursor()

getDataTypes(c)

getDataElementsInReports(c)
getLabelsForDataElements(c)

getDimensionsInReports(c)

generateOutputJSON(c)

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
