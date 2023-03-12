import findspark

from pyspark.sql import SparkSession
from lib.hdfs import Hdfs
from dfvfshadoop.hdfs_file_system import HDFSFileSystem
from dfvfshadoop.definitions import TYPE_INDICATOR_HDFS
from dfvfs.resolver.resolver import Resolver
from dfvfs.path.factory import Factory
from plaso.single_process.extraction_engine import SingleProcessEngine
from plaso.cli import log2timeline_tool
from plaso.engine import engine
from plaso.containers import artifacts
from dfvfshadoop.hdfs_path_specification import HDFSPathSpec
from dfvfs.resolver import resolver as path_resolver
from plaso.cli import log2timeline_tool
from plaso.single_process import extraction_engine

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
        self.extraction_tool = log2timeline_tool.Log2TimelineTool()
        self.extraction_engine = extraction_engine.SingleProcessEngine()

        self.rdd_path_specs = None
        self.parser_mediator = None
        self.configuration = None

        self.create_extraction_configs()

    def upload_spark_dep(self):
        self.sc.addPyFile('spark_dep/dfvfshadoop.zip')
        self.sc.addPyFile('spark_dep/lib.zip')
        # self.sc.addPyFile('spark_dep/plaso.zip')


    def create_files_path_spec_rdd(self):
        entries = self.hdfs_file_system.ListFileSystem()
        files = self.hdfs_file_system.GetOnlyFiles(entries)

        rdd_files = self.sc.parallelize(files)
        self.rdd_path_specs = rdd_files.map(lambda x: HDFSPathSpec(location=x))

        return self.rdd_path_specs

    def create_extraction_configs(self):
        self.configuration = self.extraction_tool._CreateExtractionProcessingConfiguration()
        self.parser_mediator = self.extraction_engine._CreateParserMediator(self.extraction_engine.knowledge_base,
                                                                            self.extraction_tool._resolver_context,
                                                                            self.configuration)

    def extract(self):
        rdd_path_specs = self.create_files_path_spec_rdd()
        from lib.spark_scripts import get_signature_parser

        rdd_signatures = rdd_path_specs.map(get_signature_parser)

        return rdd_signatures

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

