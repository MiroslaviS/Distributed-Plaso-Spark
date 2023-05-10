#!/bin/sh
# Every test case is run in mupltiple spark configurations
# Extract configurations:
# 1. partitions = ""
# 2. partitions = 40
# 3. partitions = 60
# 4. partitions = 80
# 5. partitions = 100
# 6. partitions = 120

#partitions=("40" "60" "80" "100" "120")
partitions=("480" "640" "800" "960")
times=()
test_case_path="$1"
filename=`basename $1 .zip`

mkdir -p $2

bash ./delete_hdfs.sh
bash ./upload_file.sh "$test_case_path" && ./upload_hdfs.sh

#bash ./extract.sh "" json "" $i > output/test_case/$filename.json
#extr_time=`cat "output/test_case/$filename.json" | grep time\" | awk '{split($0,a, ":"); print a[2]}'`
#times["None"]=$extr_time

for i in "${partitions[@]}"
do
  bash ./extract.sh "" json "" $i > "$2/$filename-$i.json" "True"

  extr_time=`cat "$2/$filename-$i.json" | grep time\" | awk '{split($0,a, ":"); print a[2]}'`
  times[$i]=$extr_time
done

echo "Extraction test case $filename results" > "$2/$filename.timing"

for i in "${partitions[@]}"
do
  echo "Partitions: $i extracted with time: ${times[$i]}" >> "$2/$filename.timing"
done