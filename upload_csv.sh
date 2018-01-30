#!/bin/sh

echo "Here are the APRA csv files"
ls *.csv
for f in *.csv
do
  echo "Deleting from server: $f"
  curl -v ftp://$url -Q "DELE SBRSoftwareDeveloperRelationship&Support@sbr.gov.au/Taxonomy/APRA drafts/$f" --user $user:$pass 2> /dev/null
  echo "Uploading $f"
  curl -T "$f" ftp://$url/SBRSoftwareDeveloperRelationship%26Support%40sbr.gov.au/Taxonomy/APRA%20drafts/ --user $user:$pass
done
