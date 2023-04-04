
from plasospark.plasowrapper import PlasoWrapper
from plasospark.sparkjobs import SparkJobFactory
from managers.distributedmanager import DistributedFileManager

class SparkPlaso:
    def __init__(self, logger):
        self.plaso = PlasoWrapper()
        self.job_factory = SparkJobFactory(self.plaso, logger)
        self.storage_manager = DistributedFileManager()

        self.logger = logger
        # self.formatter = FormatterManager.get_formatter("json")
        self.formatter = None

        self.file_entries_rdd = None
        self.path_specs_rdd = None
        self.signature_parsers_rdd = None
        self.extraction_files_rdd = None
        self.events_rdd = None
        self.warnings_rdd = None
        self.recoveries_rdd = None
        self.formatted_events_rdd = None
        self.event_source_rdd = None
        self.event_data_stream_rdd = None
        self.mediator_data_extraction_files_rdd = None
        self.extraction_result_rdd = None
        self.formatted_warnings_rdd = None
        self.formatted_recovery_rdd = None

    def extraction(self):

        if self.formatter is None:
            # No formatter means use native Plaso output -> .plaso format (SQLite table)
            self.create_start_extraction()

        self._list_hdfs_files()

        self.file_entries_rdd = self.job_factory.create_file_entry_rdd(self.path_specs_rdd)

        if not self.formatter:
            self.event_source_rdd = self.job_factory.create_event_source_rdd(self.path_specs_rdd)
            self.event_data_stream_rdd = self.job_factory.create_stream_data_event(self.path_specs_rdd)

        self.signature_parsers_rdd = self.job_factory.create_signature_parsers(self.file_entries_rdd)
        self.extraction_files_rdd = self.job_factory.filter_signature_parsers(self.signature_parsers_rdd)

        # files_parsers = [(x[0].location, x[1]) for x in self.extraction_files_rdd.collect()]
        #
        # self.logger("FILES WITH PARSERS: " + str(files_parsers))

        mediator_data = self.plaso.create_mediator_holder()
        self.mediator_data_extraction_files_rdd = self.extraction_files_rdd.map(lambda x: (x[0], x[1], mediator_data))

        extraction_data = self.mediator_data_extraction_files_rdd.repartition(7)
        self.extraction_result_rdd = self.job_factory.create_events_from_rdd(extraction_data)
        self.events_rdd, self.warnings_rdd, self.recoveries_rdd = self.job_factory.split_events_rdd(self.extraction_result_rdd)

        if self.formatter:
            self.formatted_events_rdd = self.job_factory.create_formatted_rdd(self.events_rdd, self.formatter)
            self.formatted_warnings_rdd = self.job_factory.create_formatted_rdd(self.warnings_rdd, self.formatter)
            self.formatted_recovery_rdd = self.job_factory.create_formatted_rdd(self.recoveries_rdd, self.formatter)
        else:
            # Use plaso formating
            self.create_plaso_output()

        return self.create_response()

    def create_response(self):
        if self.formatter:
            formatted_events = self.formatted_events_rdd.collect()
            # formatted_warnings = self.formatted_warnings_rdd.collect()
            # formatted_recoveries = self.formatted_recovery_rdd.collect()

            response = {'events': formatted_events,
                        # 'warnings': formatted_warnings,
                        # 'recovery': formatted_recoveries,
                        'status': f'Events formated to {self.formatter.NAME}'}
        else:
            events = self.events_rdd.collect()
            warnings = self.warnings_rdd.collect()
            recovery = self.recoveries_rdd.collect()

            response = {'number of events': len(events),
                        'number of warnings': len(warnings),
                        'number of recovery': len(recovery),
                        'status': f'Events saved to plaso format into {self.plaso.storage_file_path}'}

        return response

    def create_plaso_output(self):
        self.process_plaso_event_sources()
        self.process_event_data()
        self.process_warning_data()
        self.process_recovery_data()

        self.create_end_plaso_extraction()

    def create_start_extraction(self):
        self.plaso.create_plaso_start_containers()

    def create_end_plaso_extraction(self):
        self.plaso.create_plaso_complete_containers()

    def process_plaso_event_sources(self):
        event_sources = self.event_source_rdd.collect()
        self.plaso.add_event_source(event_sources)

    def process_event_data(self):
        events_data = self.events_rdd.collect()
        data_streams = self.event_data_stream_rdd.collect()

        self.plaso.add_event(events_data, data_streams)

        return self.plaso.process_event_data()

    def process_warning_data(self):
        warning_data = self.warnings_rdd.collect()
        self.plaso.create_plaso_warning_containers(warning_data)

    def process_recovery_data(self):
        recovery_data = self.recoveries_rdd.collect()
        self.plaso.create_plaso_recovery_containers(recovery_data)

    def _list_hdfs_files(self):
        files = self.storage_manager.get_files()
        self.path_specs_rdd = self.job_factory.create_files_path_spec_rdd(files)
