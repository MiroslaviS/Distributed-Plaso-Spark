import findspark
from pyspark.sql import SparkSession
from dfvfs.path.hdfs_path_specification import HDFSPathSpec
import json
from pyspark import SparkConf

class SparkJobFactory:
    def __init__(self, plaso, logger):
        findspark.init()

        spark = SparkSession.builder.appName("PySpark Plaso").config("spark.executor.memory", "1g").config("spark.executor.cores", "3").config("spark.python.profile", "true").getOrCreate()
        # spark = SparkSession.builder.appName("PySpark Plaso").config("spark.python.profile", "true").getOrCreate()

        self.sc = spark.sparkContext
        self.plaso = plaso
        self.logger = logger
        self.upload_spark_dep()

        self.broadcast_mediator = None
        self.broadcast_config_parser = None

    def create_broadcast_mediator(self, data):
        self.broadcast_mediator = self.sc.broadcast(data)

    def test(self, configuration):
        foo = self.sc.parallelize([configuration])

        return foo

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

        return file_entries_rdd

    def create_signature_parsers(self, file_entries_rdd):
        """

        :param file_entries_rdd:
        :return: (file_entry, [parser_names]): RDD
        """
        from helpers.spark_scripts import get_signature_parser

        config_parser = self.plaso.get_filter_expression()

        broadcast_config_parser = self.sc.broadcast(config_parser)
        self.logger("Available parser filter expression: " + config_parser)

        # Map configuration to file entry in RDD
        # signature_rdd = file_entries_rdd.map(lambda x: (x, config_parser))
        # self.logger("Calculated signatures for file entries")

        rdd_signature_parsers = file_entries_rdd.map(lambda file_entry: get_signature_parser(file_entry, broadcast_config_parser))
        self.broadcast_config_parser = broadcast_config_parser

        self.logger("Calculated parsers from from file entries signatures")
        return rdd_signature_parsers

    def create_events_from_rdd(self, all_files_rdd):
        from helpers.spark_scripts import parse

        self.logger("Starting parsing on RDDs")
        mediator = self.broadcast_mediator
        events_rdd = all_files_rdd.flatMap(lambda file: parse(file, mediator))

        return events_rdd

    def create_formatted_rdd(self, events_rdd, formatter):
        non_empty_events_rdd = events_rdd.filter(lambda events: len(events) != 0)
        formatted_events = non_empty_events_rdd.map(formatter.format)

        return formatted_events

    def filter_signature_parsers(self, signature_parsers_rdd):
        """

        :param signature_parsers_rdd:
        :return: [(path_spec, parser)]
        """
        non_sig = self.plaso.get_nonsig_parsers()
        broadcast_non_sig = self.sc.broadcast(non_sig)
        # Get files without signed signature for parsers
        non_sig_files = signature_parsers_rdd.filter(lambda x: len(x[1]) == 0)

        # Create tuple RDD with file and assigned non sig parsers
        non_sig_ext = non_sig_files.map(lambda x: (x[0], broadcast_non_sig.value))

        # Get files with signed signature for parsers
        sig_files = signature_parsers_rdd.filter(lambda x: len(x[1]) != 0)

        from helpers.spark_scripts import expand_file_parsers

        sig_rdd = sig_files.flatMap(expand_file_parsers)
        non_sig_rdd = non_sig_ext.flatMap(expand_file_parsers)

        all_files_rdd = sig_rdd.union(non_sig_rdd)

        return all_files_rdd
