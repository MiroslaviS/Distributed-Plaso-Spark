
from dfvfshadoop.definitions import TYPE_INDICATOR_HDFS
from dfvfs.resolver.resolver import Resolver
from dfvfs.path.factory import Factory
from dfvfshadoop.hdfs_path_specification import HDFSPathSpec
from plaso.cli import log2timeline_tool
from plaso.cli.log2timeline_tool import Log2TimelineTool

from plasospark.plasowrapper import PlasoWrapper
from plasospark.sparkjobs import SparkJobFactory

class SparkPlaso(log2timeline_tool.Log2TimelineTool):
    def __init__(self, logger):
        super(SparkPlaso, self).__init__()

        self.plaso = PlasoWrapper()
        self.job_factory = SparkJobFactory(self.plaso, logger)

        hdfs_path_spec = Factory.NewPathSpec(TYPE_INDICATOR_HDFS, location='namenode')
        self.hdfs_file_system = Resolver.OpenFileSystem(hdfs_path_spec)

        self.logger = logger

        self.file_entries_rdd = None
        self.path_specs_rdd = None
        self.signature_parsers_rdd = None
        self.extraction_files_rdd = None
        self.events_rdd = None

    def extraction(self):
        self.path_specs_rdd = self.job_factory.create_files_path_spec_rdd(self.hdfs_file_system)
        self.file_entries_rdd = self.job_factory.create_file_entry_rdd(self.path_specs_rdd)
        self.signature_parsers_rdd = self.job_factory.create_signature_parsers(self.file_entries_rdd)
        self.extraction_files_rdd = self.job_factory.filter_signature_parsers(self.signature_parsers_rdd)
        self.events_rdd = self.job_factory.create_events_from_rdd(self.extraction_files_rdd)

        return self.events_rdd


