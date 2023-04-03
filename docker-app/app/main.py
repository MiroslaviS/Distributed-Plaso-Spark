# -*- coding: utf-8 -*-
"""
    Flask API server for sending commands to Plaso extractor
    and uploading files to HDFS
"""
from flask import Flask, request, make_response
from managers.filemanager import LocalStorageManager

import findspark
findspark.init()    # This is necessary to be before importing sparkContext !

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = "upload_no_process"
app.config['PREPROCESSED_FOLDER'] = "upload_processed"

local_storage = LocalStorageManager(app)

from plasospark.sparkplaso import SparkPlaso

@app.route('/extract')
def spark():
    plasoSpark = SparkPlaso(app.logger.warning)

    response = plasoSpark.extraction()

    return make_response(response, 200)


@app.route("/upload/files", methods=['POST'])
def upload_files():
    if 'files[]' not in request.files:
        return make_response("Multiple files not provided, missing files[] key", 400)

    files = request.files.getlist('files[]')
    saved_files = local_storage.save_files(files)

    local_storage.preprocess_files()
    return make_response({"status": "OK, preprocessing before HDFS started", "saved_files": saved_files}, 200)


@app.route("/upload/file")
def upload_file():
    if 'file' not in request.files:
        return make_response("Missing file key in request", 400)

    file = request.files['file']
    filename = local_storage.save_file(file)

    local_storage.preprocess_files()
    return make_response({"status": "OK, preprocessing before HDFS started", "saved_file": filename}, 200)


@app.route("/upload/hdfs")
def upload_to_hdfs():
    result = local_storage.upload_to_hdfs()

    return make_response({"status": "OK", "result": result}, 200)


@app.route("/delete/hdfs")
def delete_hdfs():
    result = local_storage.delete_hdfs_files()

    return make_response({"status": "OK", "result": result}, 200)


if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True)
