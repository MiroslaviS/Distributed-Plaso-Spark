#!/bin/sh

SCRIPTNAME=$(readlink -f $0) DIRNAME=$(dirname ${SCRIPTNAME})/../
#mkdir -p "${DIRNAME}"


echo $DIRNAME
exit 0
ZIP_FILE="${DIRNAME}/site-packages.zip"

echo -n "The ZIP file will be created by: "
if which 7z; then
	ZIP="7z a -mx -xr!*.so"
elif which zip; then
	ZIP="zip -9r -x*.so"
else
	echo "No application to create ZIP files!" >&2
	exit 1
fi

if [[ -s "${1}" ]]; then
	VENV_NAME="${1}"
	shift
else
	VENV_NAME="${DIRNAME}/venv"
fi

rm -v "${ZIP_FILE}"

if [[ "${1}" == "--without-venv" ]]; then
	echo "Skipping venv site-packages (do not use --without-venv to include them)!" >&2
else
	SP_DIR=$(echo "${VENV_NAME}"/lib/python?.?/site-packages)
	if ! [[ -d "${SP_DIR}" ]]; then
		echo "No VirtualEnv directory ${VENV_NAME} and its ${SP_DIR}!" >&2
		echo "Run make-virtual-env.sh first!" >&2
		exit 1
	fi
	echo "Packing ${SP_DIR} ..." >&2
	# venv: run in a sub-shell to keep PWD after the command
	( cd ${SP_DIR} && ${ZIP} "${ZIP_FILE}" . )
	# venv/*.pth: add __init__.py as namespaces decalred in *.pth files are not supported in SparkContext.addPyFile, see https://github.com/googleapis/google-cloud-python/issues/4247
	echo "Converting *.pth namespace-package files in ${SP_DIR} into __init__.py files to enable the namespace ..." >&2
	TMP_DIR=/tmp/$$
	for I in $(grep -o "p = os\.path\.join(sys\._getframe(1).f_locals\['sitedir'\], \*('[^']*',));" ${SP_DIR}/*.pth | cut -d "'" -f 4); do
		mkdir -p "${TMP_DIR}/${I}"
		touch "${TMP_DIR}/${I}/__init__.py"
	done
	( cd ${TMP_DIR} && ${ZIP} "${ZIP_FILE}" . )
	rm -rf "${TMP_DIR}"
fi

SRC_DIR="${DIRNAME}/../../src"

echo "Packing src ..." >&2
# src: run in a sub-shell to keep PWD after the command
( cd ${SRC_DIR} && ${ZIP} "${ZIP_FILE}" . )

echo "================================================================================" >&2
echo "Binary platform-dependent builds (*.so) were not included in the ZIP file, so it can be distributed to nodes of various platforms!" >&2
echo "To use the binary parts of Python packages, install them by a package manager in each of the target nodes!" >&2
echo "The packages in the ZIP file will be utilized mostly in Spark workers, however, they can be required also in a Spark client to interpret data returned by the Spark workers, e.g., in 'print(rdd.collect())' calls." >&2
echo "================================================================================" >&2