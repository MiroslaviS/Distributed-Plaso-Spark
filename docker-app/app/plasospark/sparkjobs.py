"""
    Component for creating Spark jobs for Plaso extraction
"""

import findspark
findspark.init()

from pyspark.sql import SparkSession
from dfvfs.path.hdfs_path_specification import HDFSPathSpec
import json


class SparkJobFactory:
    """ Component for creating Spark jobs """
    def __init__(self, plaso, logger):
        """
            Initializes spark job component
        Params:
            plaso (PlasoWrapper): Wrapper for plaso log2timeline
            logger (logging.Logger): Logger for debuging purpose
        """
        spark = SparkSession.builder.appName("PySpark Plaso Testing").getOrCreate()

        self.sc = spark.sparkContext
        self.plaso = plaso
        self.logger = logger
        self.upload_spark_dep()

        self.broadcast_mediator = None
        self.broadcast_config_parser = None

    def create_broadcast_mediator(self, data):
        """
            Broadcast mediator data to Spark cluster
        Params:
            data (MediatorHolder): Parser mediator data in holder object
        """
        self.broadcast_mediator = self.sc.broadcast(data)

    def repartitionBeforeExtract(self, extraction_files, partitions_no):
        """
            Optimize number of partitions and data distribution in extraction RDDs
        Params:
            extraction_files ([(Parser, FileEntry)]): RDD with data for parsing process
            partition_no (int): Number of desired partitions
        Returns:
             ([(Parser, FileEntry)]): RDD with redistributed data and repartitioned
        """
        import random
        repartition_files = extraction_files.partitionBy(partitions_no, lambda _: random.randint(0, partitions_no-1))

        return repartition_files

    def upload_spark_dep(self):
        """
            Uploads Spark dependencies to Spark cluster
        """
        self.sc.addPyFile('spark_dep/helpers.zip')
        self.sc.addPyFile('spark_dep/mediators.zip')
        self.sc.addPyFile('spark_dep/formatters.zip')

    def create_event_source_rdd(self, path_specs):
        """
            Creates event sources from HDFS path specifications
        Params:
            path_specs ([HDFSPathSpec]): List of HDFS path specifications
        Returns:
            [FileEntryEventSource]: RDD with event sources
        """
        from helpers.spark_scripts import create_event_sources
        event_sources_rdd = path_specs.map(create_event_sources)

        return event_sources_rdd

    def create_stream_data_event(self, path_specs):
        """
            Creates data stream event from given path specifications
        Params:
            path_specs ([HDFSPathSpec]): List of HDFS path specifications
        Returns:
            [EventDataStream]: List of Data stream events
        """
        from helpers.spark_scripts import create_data_stream_event
        data_stream_event_rdd = path_specs.map(create_data_stream_event)

        return data_stream_event_rdd

    def create_files_path_spec_rdd(self, files):
        """
            Creates HDFS path specification from given files path
        Params:
            files ([files]): List of file paths from hdfs storage
        Returns:
            [HDFSPathSpec]: RDD with HDFS path specifications
        """
        rdd_files = self.sc.parallelize(files)
        rdd_path_specs = rdd_files.map(lambda x: HDFSPathSpec(location=x))

        return rdd_path_specs

    def create_file_entry_rdd(self, path_specs):
        """
            Creates file entry RDD from givne path specifications
        Params:
            path_spec ([HDFSPathSpec]): RDD with path specifications of HDFS files
        Returns:
            [HDFSPathSpec]: List of HDFS path specifications
        """
        from helpers.spark_scripts import create_file_entry_rdd
        file_entries_rdd = path_specs.map(create_file_entry_rdd)

        return file_entries_rdd

    def create_signature_parsers(self, file_entries_rdd):
        """
            Calculates file entry signature to create list of parsers for file
        Params:
            file_entries_rdd ([FileEntry]): RDD with file entries
        Returns:
             (file_entry, [parser_names]): RDD with created parsers for given file entry
        """
        from helpers.spark_scripts import get_signature_parser

        config_parser = self.plaso.get_filter_expression()

        broadcast_config_parser = self.sc.broadcast(config_parser)
        self.logger("Available parser filter expression: " + config_parser)

        rdd_signature_parsers = file_entries_rdd.map(lambda file_entry: get_signature_parser(file_entry, broadcast_config_parser))
        self.broadcast_config_parser = broadcast_config_parser

        return rdd_signature_parsers

    def create_events_from_rdd(self, all_files_rdd):
        """
            Maps parse script to RDD of files with extractors
        Params:
            all_files_rdd ([(Parser, FileEntry)]): RDD with calculated parser for FileEntry
        Returns:
            [Events]: RDD with extracted events
        """
        from helpers.spark_scripts import parse

        mediator = self.broadcast_mediator
        events_rdd = all_files_rdd.flatMap(lambda file: parse(file, mediator))

        return events_rdd

    def create_formatted_rdd(self, events_rdd, formatter):
        """
            Formate extracted events with given formatter
        Params:
            events_rdd ([EventData]): RDD with extracted event data
            formatter (Formatter): Formatter for EventData
        Returns:
            [Event]: List of formatted Events
        """
        formatted_events = events_rdd.map(formatter.format)

        return formatted_events

    def filter_signature_parsers(self, signature_parsers_rdd):
        """
            Filters non-signature files and add non-signature parsers
            to non-sig files if needed.

        Params:
            signature_parsers_rdd ([(FileEntry, [Parser]]): List of FileEntry objects with list of associated parsers
        Returns:
             [(Parser, FileEntry)]: List of FileEntries with calculated parser
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
