
from plasospark.plasowrapper import PlasoWrapper
from plasospark.sparkjobs import SparkJobFactory
from managers.distributedmanager import DistributedFileManager
from formatters.json import JsonFormatter


class SparkPlaso:
    def __init__(self, logger):
        self.plaso = PlasoWrapper()
        self.job_factory = SparkJobFactory(self.plaso, logger)
        self.storage_manager = DistributedFileManager()

        self.logger = logger
        self.formatter = JsonFormatter()

        self.file_entries_rdd = None
        self.path_specs_rdd = None
        self.signature_parsers_rdd = None
        self.extraction_files_rdd = None
        self.events_rdd = None
        self.formatted_events_rdd = None
        self.event_source_rdd = None
        self.event_data_stream_rdd = None

    def extraction(self):
        self._list_hdfs_files()

        self.file_entries_rdd = self.job_factory.create_file_entry_rdd(self.path_specs_rdd)
        self.signature_parsers_rdd = self.job_factory.create_signature_parsers(self.file_entries_rdd)
        self.extraction_files_rdd = self.job_factory.filter_signature_parsers(self.signature_parsers_rdd)
        self.events_rdd = self.job_factory.create_events_from_rdd(self.extraction_files_rdd)

        self.formatted_events_rdd = self.job_factory.create_formatted_rdd(self.events_rdd, self.formatter)

        return self.formatted_events_rdd

    def test(self):
        self.event_source_rdd = self.job_factory.create_event_source_rdd(self.path_specs_rdd)
        self.event_data_stream_rdd = self.job_factory.create_stream_data_event(self.path_specs_rdd)

        return self.event_source_rdd

    def process_plaso_event_sources(self):
        event_sources = self.event_source_rdd.collect()
        self.plaso.add_event_source(event_sources)

    def process_plaso_data_streams(self):
        data_streams = self.event_data_stream_rdd.collect()
        self.plaso.add_event_data_stream(data_streams)

    def process_event_data(self):
        events_data = self.events_rdd.collect()
        self.plaso.add_event(events_data)

        return self.plaso.process_event_data()

    def _list_hdfs_files(self):
        files = self.storage_manager.get_files()
        self.path_specs_rdd = self.job_factory.create_files_path_spec_rdd(files)
