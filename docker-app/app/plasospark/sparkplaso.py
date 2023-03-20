import findspark

from pyspark.sql import SparkSession
from dfvfshadoop.definitions import TYPE_INDICATOR_HDFS
from dfvfs.resolver.resolver import Resolver
from dfvfs.path.factory import Factory
from plaso.single_process.extraction_engine import SingleProcessEngine
from plaso.engine import engine
from dfvfshadoop.hdfs_path_specification import HDFSPathSpec
from plaso.cli import log2timeline_tool
from plaso.single_process import extraction_engine as single_extraction_engine
from plaso.cli.log2timeline_tool import Log2TimelineTool
from plaso.engine import extractors

class SparkPlaso(log2timeline_tool.Log2TimelineTool):
    def __init__(self, logger):
        super(SparkPlaso, self).__init__()

        findspark.init()
        spark = SparkSession.builder.appName("PySpark Plaso").getOrCreate()
        self.sc = spark.sparkContext

        hdfs_path_spec = Factory.NewPathSpec(TYPE_INDICATOR_HDFS, location='namenode')
        self.hdfs_file_system = Resolver.OpenFileSystem(hdfs_path_spec)

        self._artifact_definitions_path = '/app/plaso/share_artifacts/artifacts/'
        self.logger = logger

        self.upload_spark_dep()
        self.extraction_tool = Log2TimelineTool()
        self.extraction_engine = SingleProcessEngine()

        self.file_entries_rdd = None
        self.rdd_path_specs = None
        self.parser_mediator = None
        self.configuration = None
        self.rdd_signature_parsers = None

        self.create_extraction_configs()

    def upload_spark_dep(self):
        self.sc.addPyFile('spark_dep/dfvfshadoop.zip')
        self.sc.addPyFile('spark_dep/lib.zip')
        self.sc.addPyFile('spark_dep/mediators.zip')

    def test(self):
        extractor = self.extraction_engine._extraction_worker._event_extractor._parsers

        rdd = self.sc.parallelize([extractor])
        rdd2 = rdd.map(lambda x: x.get("asl_log"))

        return rdd2.collect()

    def create_files_path_spec_rdd(self):
        entries = self.hdfs_file_system.ListFileSystem()
        files = self.hdfs_file_system.GetOnlyFiles(entries)

        rdd_files = self.sc.parallelize(files)
        self.rdd_path_specs = rdd_files.map(lambda x: HDFSPathSpec(location=x))

        self.logger("Created RDD with path specifications.")
        return self.rdd_path_specs

    def create_file_entry_rdd(self):
        from lib.spark_scripts import create_file_entry_rdd
        self.file_entries_rdd = self.rdd_path_specs.map(create_file_entry_rdd)
        self.logger("Created RDD with file entries.")

    def create_file_object_rdd(self):
        self.file_objects_rdd = self.file_entries_rdd.map()

    def calculate_signature_parsers(self):
        from lib.spark_scripts import get_signature_parser

        config_parser = self.configuration.parser_filter_expression

        self.logger("Available parser filter expression: " + config_parser)

        # Map configuration to file entry in RDD
        signature_rdd = self.file_entries_rdd.map(lambda x: (x, config_parser))

        self.rdd_signature_parsers = signature_rdd.map(get_signature_parser)

        return self.rdd_signature_parsers

    def filter_signature_parsers(self):
        non_sig = self.extraction_engine._extraction_worker._event_extractor._non_sigscan_parser_names

        non_sig_files = self.rdd_signature_parsers.filter(lambda x: len(x[1]) == 0)
        non_sig_ext = non_sig_files.map(lambda x: (x[0], non_sig))

        sig_files = self.rdd_signature_parsers.filter(lambda x: len(x[1]) != 0)

        from lib.spark_scripts import expand_file_parsers
        sig_rdd = sig_files.flatMap(expand_file_parsers)
        non_sig_rdd = non_sig_ext.flatMap(expand_file_parsers)

        all_files_rdd = sig_rdd.union(non_sig_rdd)

        return all_files_rdd


    def check_metadata_files(self):
        from lib.spark_scripts import check_if_metadata
        config_parser = self.configuration.parser_filter_expression

        rdd = self.file_entries_rdd.map(lambda x: (x, config_parser))
        metadata_files = rdd.map(check_if_metadata)

        return metadata_files

    def create_extraction_configs(self):
        self.extraction_tool.ParseArguments(['--debug', '--single-process'])
        self.extraction_tool._expanded_parser_filter_expression = (self.extraction_tool._GetExpandedParserFilterExpression(
                                                                        self.extraction_engine.knowledge_base))
        self.configuration = self.extraction_tool._CreateExtractionProcessingConfiguration()
        self.parser_mediator = self.extraction_engine._CreateParserMediator(self.extraction_engine.knowledge_base,
                                                                            self.extraction_tool._resolver_context,
                                                                            self.configuration)

        from plaso.engine import worker

        self.extraction_engine._extraction_worker = worker.EventExtractionWorker(parser_filter_expression=self.configuration.parser_filter_expression)


    def create_plaso_configuration(self):
        """
            Creates extraction engine objects with its configuration used in
            creating events from sources
        :return:
        """
        extraction_engine = SingleProcessEngine()
        session = engine.BaseEngine.CreateSession()

        self._expanded_parser_filter_expression = (
            self._GetExpandedParserFilterExpression(
                extraction_engine.knowledge_base))

        enabled_parser_names = self._expanded_parser_filter_expression.split(',')

        configuration = self._CreateExtractionProcessingConfiguration()

        try:
            extraction_engine.BuildCollectionFilters(
                self._artifact_definitions_path, self._custom_artifacts_path,
                extraction_engine.knowledge_base, self._artifact_filters,
                self._filter_file)
        except Exception as exception:
            pass

        session_configuration = self._CreateExtractionSessionConfiguration(
            session, enabled_parser_names)

        self.logger(session_configuration.enabled_parser_names)

