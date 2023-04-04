#!/bin/sh

DIR=$(dirname "${0}")

. "${DIR}/spark-entrypoint-helpers.sh"
# from docker-hadoop-base/scripts/application-helpers.sh
. "${DIR}/application-helpers.sh"

set_master_url
set_system_jars
set_system_pyfiles
set_driver_ports
set_python

. "${DIR}/hadoop-set-props.sh"
. "${DIR}/spark-set-props.sh"

# from docker-hadoop-base/scripts/application-helpers.sh
wait_for
make_hdfs_dirs

export PYTHONIOENCODING="utf8"
export CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath --glob`

python3.7 /app/main.py