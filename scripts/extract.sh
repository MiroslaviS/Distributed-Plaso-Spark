#!/bin/sh

if [ -z "$1" ]
then
  plaso_output="/output.plaso"
else
  plaso_output="$1"
fi

if [ -z "$2" ]
then
  formatter="json"
else
  formatter="$2"
fi

if [ -z "$3" ]
then
  plaso_args='[\"--parsers\",\"!rplog\",\"--single-process\",\"--debug\"]'
else
  plaso_args="$3"
fi

if [ -z "$4" ]
then
  repartitions=""
else
  repartitions="$4"
fi

echo "output: $plaso_output and formatter: $formatter"
echo "plaso args: $plaso_args with repartition: $repartitions"

echo '{
    "output_file": "'$plaso_output'",
    "formatter": "'$formatter'",
    "plaso_args": '$plaso_args',
    "partitions": '$repartitions'
}'

if [ -z $repartitions ]
then
  curl --location 'localhost:5000/extract' \
  --header 'Content-Type: application/json' \
  --data '{
    "output_file": "'$plaso_output'",
    "formatter": "'$formatter'",
    "plaso_args": "'$plaso_args'"}'
else
  curl --location 'localhost:5000/extract' \
  --header 'Content-Type: application/json' \
  --data '{
    "output_file": "'$plaso_output'",
    "formatter": "'$formatter'",
    "plaso_args": "'$plaso_args'",
    "partitions": "'$repartitions'"}'
fi
