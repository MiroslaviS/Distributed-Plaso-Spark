#!/bin/sh

DIR=$(dirname "${0}")

export CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath --glob`

if [ -n "${ROLE}" ]; then
	. "${DIR}/entrypoint-${ROLE}.sh" ${@}
else
	. "${DIR}/hadoop-set-props.sh"
	. "${DIR}/spark-set-props.sh"
	exec ${@}
fi
