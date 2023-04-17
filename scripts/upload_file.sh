#!/bin/sh

curl --location 'localhost:5000/upload/files' --form 'files[]=@"'$1'"'
