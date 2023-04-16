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

cd /app

zip -r helpers.zip helpers/
zip -r mediators.zip mediators/
zip -r formatters.zip formatters/

mkdir -p /app/spark_dep

mv helpers.zip /app/spark_dep/
mv mediators.zip /app/spark_dep/
mv formatters.zip /app/spark_dep/

python3.7 /app/main.py