# -*- coding: utf-8 -*-
"""
    Flask API server for sending commands to Plaso extractor
    and uploading files to HDFS
"""
from flask import Flask, request, make_response
from managers.filemanager import LocalStorageManager

import findspark
findspark.init()
from pyspark import SparkContext

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = "upload_no_process"
app.config['PREPROCESSED_FOLDER'] = "upload_processed"

local_storage = LocalStorageManager(app)
from dfvfs.resolver.resolver import Resolver
from dfvfs.path.factory import Factory
from dfvfshadoop.definitions import TYPE_INDICATOR_HDFS
from plaso.helpers.language_tags import LanguageTagHelper
from plasospark.sparkplaso import SparkPlaso
from dfvfs.resolver.context import Context
from managers.distributedmanager import DistributedFileManager

@app.route("/test")
def main_page():
    return "Ahooj2"


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


if __name__ == "__main__":
    # from pyspark.sql import SparkSession
    # spark = SparkSession.builder.appName("PySpark Plaso").getOrCreate()
    # sc = spark.sparkContext
    # sc.addPyFile('spark_dep/helpers.zip')
    # sc.addPyFile('spark_dep/site-packages.zip')
    # sc.addPyFile('spark_dep/dfvfshadoop.zip')


    app.run(host='0.0.0.0', debug=True)

    # Spark Session and Spark Context
    # from pyspark.sql import SparkSession
    # spark = SparkSession.builder \
    #     .appName("PySpark Demo Application") \
    #     .getOrCreate()
    # sc = spark.sparkContext
    #
    # # Python version
    # import pkgutil, platform, sys
    # print("### Spark with Python version %s" % platform.python_version())
    # print("### sys.path = %s" % sys.path)
    # print("### 1st-level packages = %s" % sorted(map(lambda x: x[1], pkgutil.iter_modules())))
    #
    # # test RDD
    # print("### testing RDD.map = %s" % sc.parallelize(range(1, 10)).map(lambda x: x*2).collect())
