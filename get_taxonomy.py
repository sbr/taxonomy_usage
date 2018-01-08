#!/usr/bin/python
import ftplib
import sys
import zipfile
import os

if( "url" not in os.environ or "user" not in os.environ or "pass" not in os.environ):
    print "No url, user or pass environment variables. Quitting"
    sys.exit(1)

ftp = ftplib.FTP(os.environ['url'])
ftp.login(os.environ['user'], os.environ['pass'])

path = "/SBRSoftwareDeveloperRelationship&Support@sbr.gov.au/Taxonomy/1. Current Version/"
ftp.cwd(path)         # change directory to /pub/
filenames = ftp.nlst()
if(len(filenames) != 1):
    for filename in filenames:
        print filename
    print "Too many files in directory. Quitting"
    sys.exit(1)

taxonomyZip = filenames[0]
taxonomyFolder = taxonomyZip.replace(".zip","")

if( taxonomyFolder in os.listdir('.')):
    #print "Folder already exists"
    print taxonomyFolder + "/sbr_au"
    sys.exit(0)


#print "Downloading taxonomy: " + taxonomyZip
with open(filenames[0], 'wb') as local_file:
            ftp.retrbinary('RETR ' + path + "/" + taxonomyZip, local_file.write)
ftp.quit()

#print "Unzipping into: " + taxonomyFolder
zip_ref = zipfile.ZipFile(taxonomyZip, 'r')
zip_ref.extractall(taxonomyFolder)
zip_ref.close()

#print "done."
print taxonomyFolder  + "/sbr_au"
