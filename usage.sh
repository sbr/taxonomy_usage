#!/bin/sh

echo "Getting SBR element usage"
taxonomyFolder=`python get_taxonomy.py`
if [ $? -eq 0 ]; then
  python eeu.py ${taxonomyFolder}
  if [ $? -eq 0 ]; then
    echo "Everything seems ok"
    # find *.json -type f -print -exec cat {} \;
    ./apra_usage.py ${taxonomyFolder}
    ./upload_csv.sh

    # toying with something like
    #curl -vX POST http://ausdx.tk/api/domains/other -d @other.json --header "Content-Type: application/json"
  fi
fi
