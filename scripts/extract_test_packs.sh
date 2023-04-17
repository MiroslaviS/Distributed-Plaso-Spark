#!/bin/sh

bash ./delete_hdfs.sh

for f in ../tests/test_packs/zip/*
do
  filename=`basename $f .zip`
  bash ./upload_file.sh "$f" && ./upload_hdfs.sh
  if [ -z $1 ]
  then
    bash ./extract.sh "" json "" > output/$filename.json
  else
    bash ./extract.sh "" json "" $1> output/$filename.json
  fi

  bash ./delete_hdfs.sh
done
