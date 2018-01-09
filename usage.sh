#!/bin/sh

echo "Getting SBR element usage"
taxonomyFolder=`python get_taxonomy.py`
if [ $? -eq 0 ]; then
  python eeu.py ${taxonomyFolder}
  if [ $? -eq 0 ]; then
    echo "Everything seems ok"
    find *.json -type f -print -exec cat {} \;
  fi
fi
