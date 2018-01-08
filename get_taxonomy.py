#!/usr/bin/python
import ftplib
import sys
import zipfile
import os


sys.stderr.write("Downloading latest SBR XBRL Taxonomy.\n")
sys.stderr.write("Checking environment setup:\n")
if( "url" not in os.environ or "user" not in os.environ or "pass" not in os.environ):
    sys.stderr.write("Environment vars not seup.\n")
    print "No url, user or pass environment variables. Quitting"
    sys.exit(1)

sys.stderr.write("OK.\n")

sys.stderr.write("Logging in to remote FTP server:\n")

ftp = ftplib.FTP(os.environ['url'])
ftp.login(os.environ['user'], os.environ['pass'])
sys.stderr.write(" OK.\n")

path = "/SBRSoftwareDeveloperRelationship&Support@sbr.gov.au/Taxonomy/1. Current Version/"
ftp.cwd(path)
filenames = ftp.nlst()
if(len(filenames) != 1):
    for filename in filenames:
        print filename
    print "Too many files in directory. Quitting"
    sys.exit(1)

taxonomyZip = filenames[0]
sys.stderr.write("Latest taxonomy is: " + taxonomyZip +"\n")
taxonomyFolder = taxonomyZip.replace(".zip","")

if( taxonomyFolder in os.listdir('.') ):
    print taxonomyFolder + "/sbr_au"
    sys.exit(0)

sys.stderr.write("Downloading:\n")

with open(filenames[0], 'wb') as local_file:
            ftp.retrbinary('RETR ' + path + "/" + taxonomyZip, local_file.write)
ftp.quit()
sys.stderr.write(" OK.\n")


sys.stderr.write("Unzipping:\n")

zip_ref = zipfile.ZipFile(taxonomyZip, 'r')
zip_ref.extractall(taxonomyFolder)
zip_ref.close()
sys.stderr.write("OK.\n")


print taxonomyFolder  + "/sbr_au"
