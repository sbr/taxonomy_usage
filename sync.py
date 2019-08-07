#!/usr/bin/python

import json, sys, requests

def get_definition_json_names():
    names = sys.argv[1:]
    names.remove('syntaxes.json')
    return names

def process_definitions_file(name):
    global procCount
    with open(name) as json_file:
        data = json.load(json_file)
        domain = data['acronym']
        print "Loading domain", domain
        for p in data['content']:
            procCount = procCount + 1
            dc_defn = convert_json_to_definition_catalogue_entry(p, domain)
            id = p['identifier'].replace("http://api.gov.au/definition/","")
            resp = requests.post("https://api.gov.au/repository/definitions/definition/" + id, json=dc_defn, auth=('',''))
            if procCount % 100 == 0: print str(procCount) + " / " + str(defCount)
            if procCount == defCount: print str(procCount) + " / " + str(defCount)



def add_to_count_of_definitoins(name):
    global defCount
    with open(name) as json_file:
        data = json.load(json_file)
        for p in data['content']:
            defCount = defCount + 1

def convert_json_to_definition_catalogue_entry(p, domainAcronym):
    """
data class NewDefinition(
         var name: String = "",
         var domain: String = "",
         var status: String = "",
         var definition: String = "",
         var guidance: String = "",
         var identifier: String = "",
         var usage: Array<String> = arrayOf(),
         var values: Array<String> = arrayOf(),
         var datatype: DataType = DataType("", mapOf()),
         var domainAcronym: String = "",
         var sourceURL: String = "",
         var version: String = ""
 )

{
    "status": "Standard",
    "definition": "",
    "domain": "Financial Statistics",
    "name": "Assets Loans And Receivables Lease Financing Gross Total Amount",
    "datatype": { "type": "float" },
    "usage": [ "Australian Prudential Regulation Agency" ],
    "identifier": "http://dxa.gov.au/definition/fs/de1016"
}
    """

    defn = {}
    defn['name'] = p['name']
    defn['domain'] = p['domain']
    defn['status'] = p['status']
    defn['definition'] = p.get('definition', "")
    defn['guidance'] = p.get('guidance',"")
    defn['identifier'] = p['identifier']
    defn['usage'] = p.get('usage', [])
    defn['values'] = p['datatype'].get('values',[])
    defn['datatype'] = p['datatype']
    defn['domainAcronym'] = domainAcronym
    defn['sourceURL'] = ""
    defn['version'] = ""
    return defn



defCount = 0
for name in get_definition_json_names():
    add_to_count_of_definitoins(name)

procCount = 0
for name in get_definition_json_names():
    process_definitions_file(name)


with open('syntaxes.json', 'r') as myfile:
    print "loading syntaxes"
    text = myfile.read()
    data = json.loads(text)
    count = 0
    size = len(data)
    for syntax in data:
        id = syntax['identifier']
        theSyntax = {'wrapper': syntax['syntax']}
        resp = requests.post("https://api.gov.au/repository/definitions/syntax/?id=" + id, json=theSyntax, auth=('',''))
        count = count + 1
        if count % 100 == 0: print str(count) + " / " + str(size)
        if count == size:print str(count) + " / " + str(size)
