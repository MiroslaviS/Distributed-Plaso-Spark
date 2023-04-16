
from plasospark.plasowrapper import PlasoWrapper
from plasospark.sparkjobs import SparkJobFactory
from managers.distributedmanager import DistributedFileManager
from formatters.manager import FormatterManager


class SparkPlaso:
    def __init__(self, logger, formatter=None, output_file=None, plaso_args=None, partitions=None):
        self.plaso = PlasoWrapper(storage_file=output_file, plaso_arguments=plaso_args)
        self.job_factory = SparkJobFactory(self.plaso, logger)
        self.storage_manager = DistributedFileManager()
        self.partitions = partitions
        self.logger = logger

        if formatter:
            self.formatter = FormatterManager.get_formatter(formatter)
        else:
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

        mediator_data = self.plaso.create_mediator_holder()
        self.job_factory.create_broadcast_mediator(mediator_data)

        self.logger("Number of patitions: " + str(self.extraction_files_rdd.getNumPartitions()))

        if self.partitions:
            extraction_data = self.extraction_files_rdd.repartition(int(self.partitions))
            self.extraction_result_rdd = self.job_factory.create_events_from_rdd(extraction_data)
            self.logger("Number of partition after repartition: " + str(extraction_data.getNumPartitions()))
        else:
            self.extraction_result_rdd = self.job_factory.create_events_from_rdd(self.extraction_files_rdd)

        if self.formatter:
            self.formatted_events_rdd = self.job_factory.create_formatted_rdd(self.extraction_result_rdd, self.formatter)

        return self.create_response()

    def create_response(self):
        if self.formatter:
            self.logger("Create response with formatter !")
            formatted_events = self.formatted_events_rdd.collect()

            response = {'events': formatted_events,
                        'status': f'Events formated to {self.formatter.NAME}'}
        else:
            self.logger("Create response with plaso tools!")
            events = self.extraction_result_rdd.collect()

            self.create_plaso_output(events)
            response = {'number of events': len(self.events_data),
                        'number of warnings': len(self.warning_data),
                        'number of recovery': len(self.recovery_data),
                        'status': f'Events saved to plaso format into {self.plaso.storage_file_path}'}

        return response

    def create_plaso_output(self, events):
        from plaso.containers import warnings

        self.events_data = [event for event in events if (not isinstance(event, warnings.ExtractionWarning) and not isinstance(event, warnings.RecoveryWarning))]
        self.warning_data = [event for event in events if isinstance(event, warnings.ExtractionWarning) ]
        self.recovery_data = [event for event in events if isinstance(event, warnings.RecoveryWarning) ]

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
        data_streams = self.event_data_stream_rdd.collect()

        self.plaso.add_event(self.events_data, data_streams)

        return self.plaso.process_event_data()

    def process_warning_data(self):
        self.plaso.create_plaso_warning_containers(self.warning_data)

    def process_recovery_data(self):
        self.plaso.create_plaso_recovery_containers(self.recovery_data)

    def _list_hdfs_files(self):
        files = self.storage_manager.get_files()
        self.path_specs_rdd = self.job_factory.create_files_path_spec_rdd(files)
