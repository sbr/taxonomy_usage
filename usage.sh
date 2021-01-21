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
    ./stats.py ${taxonomyFolder}> stats.txt
    echo "Deleting from server: stats.txt"
    curl -v ftp://$url -Q "DELE SBR/Taxonomy/stats.txt" --user $user:$pass 2> /dev/null
    echo "Uploading stats.txt"
    curl -T "stats.txt" ftp://$url/SBR/Taxonomy/ --user $user:$pass

    # toying with something like
    #curl -vX POST http://ausdx.tk/api/domains/other -d @other.json --header "Content-Type: application/json"
  fi
fi
