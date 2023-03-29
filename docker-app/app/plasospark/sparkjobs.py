import findspark
from pyspark.sql import SparkSession
from dfvfshadoop.hdfs_path_specification import HDFSPathSpec
import json


class SparkJobFactory:
    def __init__(self, plaso, logger):
        findspark.init()
        spark = SparkSession.builder.appName("PySpark Plaso").getOrCreate()
        self.sc = spark.sparkContext
        self.plaso = plaso
        self.logger = logger

        self.upload_spark_dep()

    def upload_spark_dep(self):
        self.sc.addPyFile('spark_dep/dfvfshadoop.zip')
        self.sc.addPyFile('spark_dep/helpers.zip')
        self.sc.addPyFile('spark_dep/mediators.zip')
        self.sc.addPyFile('spark_dep/formatters.zip')

    def create_event_source_rdd(self, path_specs):
        from helpers.spark_scripts import create_event_sources
        event_sources_rdd = path_specs.map(create_event_sources)

        return event_sources_rdd

    def create_stream_data_event(self, path_specs):
        from helpers.spark_scripts import create_data_stream_event
        data_stream_event_rdd = path_specs.map(create_data_stream_event)

        return data_stream_event_rdd

    def create_files_path_spec_rdd(self, files):
        rdd_files = self.sc.parallelize(files)
        rdd_path_specs = rdd_files.map(lambda x: HDFSPathSpec(location=x))

        self.logger("Created RDD with path specifications.")
        return rdd_path_specs

    def create_file_entry_rdd(self, path_specs):
        from helpers.spark_scripts import create_file_entry_rdd
        file_entries_rdd = path_specs.map(create_file_entry_rdd)
        self.logger("Created RDD with file entries.")

        return file_entries_rdd

    def create_signature_parsers(self, file_entries_rdd):
        from helpers.spark_scripts import get_signature_parser

        config_parser = self.plaso.get_filter_expression()

        self.logger("Available parser filter expression: " + config_parser)

        # Map configuration to file entry in RDD
        signature_rdd = file_entries_rdd.map(lambda x: (x, config_parser))

        rdd_signature_parsers = signature_rdd.map(get_signature_parser)

        return rdd_signature_parsers

    def create_events_from_rdd(self, all_files_rdd):
        from helpers.spark_scripts import parse

        events_rdd = all_files_rdd.flatMap(parse)

        return events_rdd

    def create_formatted_rdd(self, events_rdd, formatter):
        formatted_rdd = events_rdd.map(formatter.format)

        return formatted_rdd

    def filter_signature_parsers(self, signature_parsers_rdd):
        non_sig = self.plaso.get_nonsig_parsers()

        non_sig_files = signature_parsers_rdd.filter(lambda x: len(x[1]) == 0)
        non_sig_ext = non_sig_files.map(lambda x: (x[0], non_sig))

        sig_files = signature_parsers_rdd.filter(lambda x: len(x[1]) != 0)

        from helpers.spark_scripts import expand_file_parsers

        sig_rdd = sig_files.flatMap(expand_file_parsers)
        non_sig_rdd = non_sig_ext.flatMap(expand_file_parsers)

        all_files_rdd = sig_rdd.union(non_sig_rdd)

        return all_files_rdd
