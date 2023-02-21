#!/bin/sh

# For Spark properties, see
# https://spark.apache.org/docs/latest/configuration.html
# https://spark.apache.org/docs/latest/monitoring.html#spark-configuration-options

# BUG in spark.jars and spark.submit.pyfiles, use --jars and --py-files, see https://issues.cloudera.org/browse/LIVY-220

expand_spark_jars_dirs() {
	local result
	for I in $(echo "${PROP_SPARK_spark_jars}" | tr ',' ' '); do
		if [ -d "${I}" ]; then
			result="${result},$(ls -d ${I}/*.jar 2>/dev/null | tr '\n' ',' | sed 's/,$//')"
		else
			result="${result},${I}"
		fi
	done
	# remove directories as the files will be uploaded into /tmp/spark-*/userFiles-* by spark.files conf option
	export PROP_SPARK_spark_jars=`echo "${result#,}" | sed 's:\(^\|,\)[^,]*/:\1:g'`
	# to upload files
	result="${PROP_SPARK_spark_files}${result}"
	export PROP_SPARK_spark_files="${result#,}"
}

expand_spark_python_dirs() {
	local result
	for I in $(echo "${PROP_SPARK_spark_submit_pyFiles}" | tr ',' ' '); do
		if [ -d "${I}" ]; then
			result="${result},$(ls -d ${I}/*.zip ${I}/*.egg ${I}/*.py 2>/dev/null | tr '\n' ',' | sed 's/,$//')"
		else
			result="${result},${I}"
		fi
	done
	# remove directories as the files will be uploaded into /tmp/spark-*/userFiles-* by spark.files conf option
	export PROP_SPARK_spark_submit_pyFiles=`echo "${result#,}" | sed 's:\(^\|,\)[^,]*/:\1:g'`
	# to upload files
	result="${PROP_SPARK_spark_files}${result}"
	export PROP_SPARK_spark_files="${result#,}"
}

set_master_url() {
	if [ -n "${PROP_SPARK_spark_master}" ]; then
		export MASTER_URL="${PROP_SPARK_spark_master}"
	elif [ -z "${MASTER_URL}" ]; then
		export MASTER_URL="local"
	fi
	export PROP_SPARK_spark_master="${MASTER_URL}"
}

set_system_jars() {
	if [ -n "${PROP_SPARK_spark_jars}" ]; then
		export SPARK_SYSTEM_JARS="${PROP_SPARK_spark_jars}"
	elif [ -n "${SPARK_SYSTEM_JARS}" ]; then
		export PROP_SPARK_spark_jars="${SPARK_SYSTEM_JARS}"
	fi
	expand_spark_jars_dirs
}

set_system_pyfiles() {
	if [ -n "${PROP_SPARK_spark_submit_pyFiles}" ]; then
		export SPARK_SYSTEM_PYFILES="${PROP_SPARK_spark_submit_pyFiles}"
	elif [ -n "${SPARK_SYSTEM_PYFILES}" ]; then
		export PROP_SPARK_spark_submit_pyFiles="${SPARK_SYSTEM_PYFILES}"
	fi
	expand_spark_python_dirs
}

set_spark_jars() {
	PROP_SPARK_spark_jars="${SPARK_JARS}"
	PROP_SPARK_spark_files=
	expand_spark_jars_dirs
	export SPARK_JARS="${PROP_SPARK_spark_files}"
}

set_spark_pyfiles() {
	PROP_SPARK_spark_submit_pyFiles="${SPARK_PYFILES}"
	PROP_SPARK_spark_files=
	expand_spark_python_dirs
	export SPARK_PYFILES="${PROP_SPARK_spark_files}"
}

set_driver_ports() {
	# Set client driver's opened ports from communicating with a Spark cluster (the port numbers must be allowed in the client's firewall)
	# see https://spark.apache.org/docs/latest/configuration.html#networking
	# * spark.driver.port used for communicating of the client driver with the executors and the standalone Master
	[ -z "${PROP_SPARK_spark_driver_port}" ] \
	&& export PROP_SPARK_spark_driver_port=64001
	# * spark.driver.host as hostname or IP address for the driver used for communicating with the executors and the standalone Master
	#   (default `hostname` is not enough in Kubernetes cluster where the standalone Master and the app's driver can be in different namespaces, that is can have different domains
	if [ -n "${PROP_SPARK_spark_driver_host}" ]; then
		export SPARK_PUBLIC_DNS="${PROP_SPARK_spark_driver_host}"
	elif [ -z "${SPARK_PUBLIC_DNS}" ]; then
		if [ -z "${SPARK_PUBLIC_HOSTNAME}${SPARK_PUBLIC_DOMAIN}" ]; then
			# default FQDN if neither hostname nore domain is defined
			SPARK_PUBLIC_DNS="$(hostname --fqdn)"
		else
			[ -z "${SPARK_PUBLIC_HOSTNAME}" ] && SPARK_PUBLIC_HOSTNAME="$(hostname --short)"
			[ -z "${SPARK_PUBLIC_DOMAIN}" ] && SPARK_PUBLIC_DOMAIN="$(hostname --domain)"
			# hostname.domain if the domain is set or can be obtained, just hostname otherwise (set or obtained)
			[ -n "${SPARK_PUBLIC_DOMAIN}" ] && SPARK_PUBLIC_DNS="${SPARK_PUBLIC_HOSTNAME}.${SPARK_PUBLIC_DOMAIN}" || SPARK_PUBLIC_DNS="${SPARK_PUBLIC_HOSTNAME}"
		fi
	fi
	export PROP_SPARK_spark_driver_host="${SPARK_PUBLIC_DNS}"
	# * spark.driver.blockManager.port used for providing an interface of the client driver's block manager for uploading and fetching blocks (i.e., a key-value store of blocks of data)
	[ -z "${PROP_SPARK_spark_driver_blockManager_port}" ] \
	&& export PROP_SPARK_spark_driver_blockManager_port=64017
	# * spark.port.maxRetries to set a number of retries when binding to a port before giving up (each subsequent retry will increment the port used in the previous attempt by 1)
	[ -z "${PROP_SPARK_spark_port_maxRetries}" ] \
	&& export PROP_SPARK_spark_port_maxRetries=16
	# * spark.ui.port for your application's dashboard, which shows memory and workload data
	if [ -n "${PROP_SPAKR_spark_ui_port}" ]; then
		export WEBUI_PORT="${PROP_SPAKR_spark_ui_port}"
	elif [ -z "${WEBUI_PORT}" ]; then
		export WEBUI_PORT=4040
	fi
	export PROP_SPAKR_spark_ui_port="${WEBUI_PORT}"
}

set_python() {
	# Set Python interpreter on both the driver/client and executors/workers
	# see https://spark.apache.org/docs/latest/configuration.html#runtime-environment
	if [ -n "${PROP_SPARK_spark_pyspark_driver_python}" ]; then
		export SPARK_PYTHON="${PROP_SPARK_spark_pyspark_driver_python}"
	elif [ -n "${PROP_SPARK_spark_pyspark_python}" ]; then
		export SPARK_PYTHON="${PROP_SPARK_spark_pyspark_driver_python}"
	else
		export SPARK_PYTHON="python2"
	fi
	# * spark.pyspark.driver.python to set Python binary executable to use for PySpark in driver
	export PROP_SPARK_spark_pyspark_driver_python="${SPARK_PYTHON}"
	# * spark.pyspark.python to set Python binary executable to use for PySpark in executors (and also in the driver if spark.pyspark.driver.python unset)
	export PROP_SPARK_spark_pyspark_python="${SPARK_PYTHON}"
}
