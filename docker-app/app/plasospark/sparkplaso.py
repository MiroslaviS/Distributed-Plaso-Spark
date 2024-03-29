"""
    Main component for controling the Plaso extraction with Plaso extractors
    and Spark jobs
"""
from plasospark.plasowrapper import PlasoWrapper
from plasospark.sparkjobs import SparkJobFactory
from managers.distributedmanager import DistributedFileManager
from formatters.manager import FormatterManager
import time


class SparkPlaso:
    """ Sparkplaso tool controller"""

    def __init__(self, logger, formatter=None, output_file=None, plaso_args=None, partitions=None, extraction_test=False):
        """
            Initializes a Sparkplaso tool
        Params:
            logger (logging.Logger): Flask logger for logging
            formatter (Optional[str]): Name of the preferred formatter
            output_file (Optional[str]): Path for Plaso output file
            plaso_args (Optional[[str]]): Arguments for Plaso tools wrapper
            partitions (Optional[str]): Number of partitions used in repartition
            extraction_test (Optional[str]): Flag for testing purpose
        """
        self.plaso = PlasoWrapper(storage_file=output_file, plaso_arguments=plaso_args)
        self.job_factory = SparkJobFactory(self.plaso, logger)
        self.storage_manager = DistributedFileManager()
        self.partitions = partitions
        self.logger = logger

        if formatter:
            self.formatter = FormatterManager.get_formatter(formatter)
        else:
            self.formatter = None

        self.extraction_test = extraction_test
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
        self.start_time = None
        self.end_time = None
        self.test_events_size = None

    def show_partitions(self, data):
        """
            Logs data_skew information about the given RDD
        Params:
            data (RDD): Data stored in Spark RDD object
        """
        skew_test = data.glom().map(len).collect()  # get length of each partition
        skew_string = f"BEFORE REPART {min(skew_test)}, {max(skew_test)}, {sum(skew_test) / len(skew_test)}, {len(skew_test)}\n" \
                      f"Array of partitions: {str(skew_test)}"  # check if skewed
        self.logger(skew_string)

    def extraction(self):
        """
            Starts the extraction process from HDFS stored data
        Returns:
            dict: Result of the extraction
        """
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

        if self.extraction_test:
            # Show partition data distribution before repartition
            self.show_partitions(self.extraction_files_rdd)

        if self.partitions:
            extraction_data = self.job_factory.repartitionBeforeExtract(self.extraction_files_rdd, int(self.partitions))

            if self.extraction_test:
                self.show_partitions(extraction_data)

            self.extraction_result_rdd = self.job_factory.create_events_from_rdd(extraction_data)

            self.start_time = time.time()
            if self.extraction_test:
                extracted_events = self.extraction_result_rdd.collect()
                self.test_events_size = len(extracted_events)
                self.extraction_time = time.time() - self.start_time

        else:
            self.extraction_result_rdd = self.job_factory.create_events_from_rdd(self.extraction_files_rdd)

            if self.extraction_test:
                self.start_time = time.time()
                extracted_events = self.extraction_result_rdd.collect()
                self.test_events_size = len(extracted_events)
                self.extraction_time = time.time() - self.start_time

        if self.formatter:
            self.formatted_events_rdd = self.job_factory.create_formatted_rdd(self.extraction_result_rdd, self.formatter)

        return self.create_response()

    def create_response(self):
        """
            Creates extraction data as response in JSON format
        Returns:
            dict: Result from extraction
        """
        if self.formatter:
            if self.extraction_test:
                response = {'status': f'Events formated to {self.formatter.NAME}',
                            'extraction_time': self.extraction_time,
                            'events': self.test_events_size},

            else:
                formatted_events = self.formatted_events_rdd.collect()
                self.end_time = time.time()

                response = {'events': formatted_events,
                            'status': f'Events formated to {self.formatter.NAME}',
                            'time': self.end_time - self.start_time}

        else:
            events = self.extraction_result_rdd.collect()

            self.create_plaso_output(events)
            response = {'number of events': len(self.events_data),
                        'number of warnings': len(self.warning_data),
                        'number of recovery': len(self.recovery_data),
                        'status': f'Events saved to plaso format into {self.plaso.storage_file_path}'}

        return response

    def create_plaso_output(self, events):
        """
            Creates Plaso formated events and save them into plaso output file
        Args:
            events ([EventData|ExtractionWarning|RecoveryWarning])
        """
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
        """
            Creates Plaso start of extraction and add to extraction container
        """
        self.plaso.create_plaso_start_containers()

    def create_end_plaso_extraction(self):
        """
            Creates Plaso end of extraction and add to extraction container
        """
        self.plaso.create_plaso_complete_containers()

    def process_plaso_event_sources(self):
        """
            Collects event sources and add the sources
            into Plaso container
        """
        event_sources = self.event_source_rdd.collect()
        self.plaso.add_event_source(event_sources)

    def process_event_data(self):
        """
            Collects data stream data and add the extracted data
            to Plaso container and process the event data from extraction
        """
        data_streams = self.event_data_stream_rdd.collect()

        self.plaso.add_event(self.events_data, data_streams)

        self.plaso.process_event_data()

    def process_warning_data(self):
        """
            Calls Plasowrapper to process warning data
            from extraction
        """
        self.plaso.create_plaso_warning_containers(self.warning_data)

    def process_recovery_data(self):
        """
            Calls Plasowrapper to process recovery warning
            data from extraction
        """
        self.plaso.create_plaso_recovery_containers(self.recovery_data)

    def _list_hdfs_files(self):
        """
            List all files stored in hdfs for extraction
            and creates RDD from listes files as [HDFSPathSpec]
        """
        files = self.storage_manager.get_files()
        self.path_specs_rdd = self.job_factory.create_files_path_spec_rdd(files)
