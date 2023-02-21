#!/bin/sh

DIR=$(dirname "${0}")

. "${DIR}/spark-entrypoint-helpers.sh"

set_master_url
set_spark_jars
set_spark_pyfiles

export CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath --glob`

[ -z "${SPARK_APP}" ] && SPARK_APP="/app/main.py /app/main.jar"

echo "*** Using Spark master: ${MASTER_URL}"
echo "*** Using JARs: ${SPARK_JARS}"
echo "*** Using Python files: ${SPARK_PYFILES}"
echo "*** Running Spark application: ${SPARK_APP}"

if [ "${1}" = "shell" ]; then
	shift
	exec ${SPARK_HOME}/bin/pyspark \
	--master "${MASTER_URL}" \
	--jars "${SPARK_JARS}" \
	--py-files "${SPARK_PYFILES}" \
	$@

	echo "SHELL PICOVINA"
	exit $?	# just to be sure
elif [ $# -ge 1 ]; then
  echo "NOT SHELL picovina"
	MAIN=${1}
	shift
else
  echo "ELSE VETVA"
	MAIN=$(ls -d ${SPARK_APP} 2>/dev/null | head -1)
	echo "MAIN: ${MAIN}"
fi

echo "SPARK SUBMIT"
exec ${SPARK_HOME}/bin/spark-submit \
--master "${MASTER_URL}" \
--jars "${SPARK_JARS}" \
--py-files "${SPARK_PYFILES}" \
"${MAIN}" $@
