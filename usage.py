import subprocess
import sys
import traceback
import os

taxonomyFolder = ""

try:
    print "Getting taxonomy from sharefile"
    taxonomyFolder = subprocess.check_output("python get_taxonomy.py", shell=True)
    print "The taxonomy folder is " + taxonomyFolder
    try:
        print "Extracting element usage"
        subprocess.check_output("python eeu.py " + taxonomyFolder, shell=True)
    except:
        traceback.print_exc()
except:
    print "Something went wrong getting taxonomy.\nException was:\n"
    traceback.print_exc()
