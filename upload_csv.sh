#!/bin/sh

echo "Here are the APRA csv files"
ls *.csv
for f in *.csv
do
  echo "Deleting from server: $f"
  curl -v ftp://$url -Q "DELE SBR/Taxonomy/APRA drafts/$f" --user $user:$pass 2> /dev/null
  echo "Uploading $f"
  curl -T "$f" ftp://$url/SBR/Taxonomy/APRA%20drafts/ --user $user:$pass
done
