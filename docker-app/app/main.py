# -*- coding: utf-8 -*-
"""
    Flask API server for sending commands to Plaso extractor
    and uploading files to HDFS
"""
from flask import Flask, request, make_response
from managers.localmanager import LocalStorageManager
from managers.distributedmanager import DistributedFileManager
import findspark
findspark.init()    # This is necessary to be before importing sparkContext !

from plasospark.sparkplaso import SparkPlaso

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = "upload_no_process"
app.config['PREPROCESSED_FOLDER'] = "upload_processed"

local_storage = LocalStorageManager(app)
hdfs_storage = DistributedFileManager()

@app.route('/extract')
def spark():
    output_file = request.args.get('output_file')
    formatter = request.args.get('formatter')
    plaso_args = request.args.get('plaso_args')
    partitions = request.args.get('partitions')

    if plaso_args:
        plaso_args = eval(request.args.get('plaso_args'))

    plaso_spark = SparkPlaso(app.logger.warning,
                            formatter=formatter,
                            output_file=output_file,
                            plaso_args=plaso_args,
                            partitions=partitions)

    response = plaso_spark.extraction()

    return make_response({"args": response}, 200)


@app.route("/upload/files", methods=['POST'])
def upload_files():
    if 'files[]' not in request.files:
        return make_response("Multiple files not provided, missing files[] key", 400)

    files = request.files.getlist('files[]')
    local_storage.save_files(files)

    saved_files = local_storage.preprocess_files()
    local_storage.clear_local_upload_folder()
    return make_response({"status": "OK, preprocessing before HDFS started", "saved_files": saved_files}, 200)


@app.route("/upload/file")
def upload_file():
    if 'file' not in request.files:
        return make_response("Missing file key in request", 400)

    file = request.files['file']
    filename = local_storage.save_file(file)

    local_storage.preprocess_files()
    local_storage.delete_folder(local_storage.upload_folder)
    return make_response({"status": "OK, preprocessing before HDFS started", "saved_file": filename}, 200)


@app.route("/upload/hdfs")
def upload_to_hdfs():
    result = hdfs_storage.upload_to_hdfs(app.config['PREPROCESSED_FOLDER'])
    local_storage.delete_folder(local_storage.preprocessed_folder)

    return make_response({"status": "OK", "result": result}, 200)


@app.route("/delete/hdfs")
def delete_hdfs():
    result = hdfs_storage.delete_hdfs_files()

    return make_response({"status": "OK", "result": result}, 200)


if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True)
