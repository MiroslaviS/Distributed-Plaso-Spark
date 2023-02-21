#!/bin/sh

URL="http://localhost:${WEBUI_PORT:-4040}/"
if which wget; then
	wget -qO /dev/null "${URL}" || exit 1
elif which curl; then
	curl -LfsSo /dev/null "${URL}" || exit 1
else
	echo "Missing wget or curl, cannot perform the health-check." >&2
	exit 0
fi
