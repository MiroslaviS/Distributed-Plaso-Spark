"""
    Plaso component wrapping the Original Plaso components
"""
from plaso.single_process.extraction_engine import SingleProcessEngine
from plaso.parsers import manager as parsers_manager
from plaso.engine import worker
from plaso.cli import log2timeline_tool
from plaso.storage import factory as storage_factory
from plaso.engine import engine, timeliner
import collections
from plaso.containers import counts


class PlasoWrapper(log2timeline_tool.Log2TimelineTool):
    """ Wrapper for Plaso log2timeline tool"""

    def __init__(self, storage_file=None, plaso_arguments=None):
        """

        Params:
            storage_file (Optional[str]): Path for Plaso output file
            plaso_arguments (Optional[[str]]): Arguments for Plaso Log2Timeline tools
        """
        super(PlasoWrapper, self).__init__()

        if plaso_arguments is None:
            plaso_arguments = ['--debug', '--single-process', '--parsers', '!rplog']    #rplog causes Spark jobs to not finish

        if storage_file is None:
            storage_file = '/output.plaso'

        self._artifact_definitions_path = '/app/plaso/share_artifacts/artifacts/'
        self.extraction_engine = SingleProcessEngine()

        self.configuration = None
        self.parser_mediator = None
        self.parsers = None
        self.non_sig_parsers = None
        self.storage_writer = None
        self.session = None

        self.storage_file_path = storage_file
        self.create_extraction_configs(plaso_arguments)

    def create_extraction_configs(self, plaso_arguments):
        """
            Creates all necessary Plaso components for extraction
        Params:
            plaso_arguments ([str]): Arguments for Plaso Log2Timeline tool initialization
        """
        self.ParseArguments(plaso_arguments)
        self._expanded_parser_filter_expression = (self._GetExpandedParserFilterExpression(
                                                                        self.extraction_engine.knowledge_base))
        self.configuration = self._CreateExtractionProcessingConfiguration()
        self.configuration.data_location = "/plaso/data"

        self.parser_mediator = self.extraction_engine._CreateParserMediator(self.extraction_engine.knowledge_base,
                                                                            self._resolver_context,
                                                                            self.configuration)
        self.extraction_engine._extraction_worker = worker.EventExtractionWorker(parser_filter_expression=self.configuration.parser_filter_expression)
        self.parsers = parsers_manager.ParsersManager.GetParserObjects(parser_filter_expression=self.configuration.parser_filter_expression)

        self.non_sig_parsers = self.extraction_engine._extraction_worker._event_data_extractor._non_sigscan_parser_names

        self.storage_writer = storage_factory.StorageFactory.CreateStorageWriter(self._storage_format)
        self.storage_writer.Open(path=self.storage_file_path)
        self.parser_mediator.SetStorageWriter(self.storage_writer)

        self.extraction_engine._event_data_timeliner = timeliner.EventDataTimeliner(self.extraction_engine.knowledge_base,
                                                                                    data_location=self.configuration.data_location,
                                                                                    preferred_year=self.configuration.preferred_year)
        self.extraction_engine._parsers_counter = collections.Counter({
                                                parser_count.name: parser_count
                                                for parser_count in self.storage_writer.GetAttributeContainers('parser_count')})

    def create_mediator_holder(self):
        """ Creates Holder object for Plaso ParserMediator objects
            and add the necessary data to the holder object

            Returns:
                MediatorHolder: Holder for ParserMediator data
        """
        from mediators import holder

        mediator_holder = holder.MediatorHolder(self.extraction_engine.knowledge_base,
                                                self.configuration,
                                                self.extraction_engine.collection_filters_helper)

        return mediator_holder

    def create_plaso_start_containers(self):
        """
            Creates session representing the start of Plaso
            extraction and configures the extraction parameters
        """
        self.session = engine.BaseEngine.CreateSession()
        enabled_parsers = self._expanded_parser_filter_expression.split(',')

        self.session.artifact_filters = self._artifact_filters
        self.session.command_line_arguments = self._command_line_arguments
        self.session.debug_mode = self._debug_mode
        self.session.enabled_parser_names = enabled_parsers
        self.session.extract_winevt_resources = self._extract_winevt_resources
        self.session.filter_file_path = self._filter_file
        self.session.parser_filter_expression = self._parser_filter_expression
        self.session.preferred_codepage = self._preferred_codepage
        self.session.preferred_encoding = self.preferred_encoding
        self.session.preferred_language = self._preferred_language or 'en-US'
        self.session.preferred_time_zone = self._preferred_time_zone
        self.session.preferred_year = self._preferred_year

        session_start = self.session.CreateSessionStart()

        self.storage_writer.AddAttributeContainer(session_start)
        session_configuration = self.session.CreateSessionConfiguration()

        self.storage_writer.AddAttributeContainer(session_configuration)

        system_configuration = self.extraction_engine.knowledge_base.GetSystemConfigurationArtifact()
        self.storage_writer.AddAttributeContainer(system_configuration)

    def create_plaso_complete_containers(self):
        """
            Creates session complete representing the end of
            Plaso extraction and closes the Plaso storage
        """
        session_completed = self.session.CreateSessionCompletion()
        self.storage_writer.AddAttributeContainer(session_completed)

        self.storage_writer.Close()

    def create_plaso_warning_containers(self, warning_sources):
        """
            Creates Attribute Containers for extraction warnings
            from extraction
        Params:
            warning_sources ([ExtractionWarning]): Warning created in extraction
        """
        for warning in warning_sources:
            self.storage_writer.AddAttributeContainer(warning)
            self.parser_mediator._number_of_extraction_warnings += 1

    def create_plaso_recovery_containers(self, recovery_sources):
        """
            Creates Attribute Containers for extraction recovery
            warnings data from extraction
        Params:
            recovery_sources ([RecoveryWarning]): Recovery Warnings created in extraction
        """
        for recovery in recovery_sources:
            self.storage_writer.AddAttributeContainer(recovery)
            self.parser_mediator._number_of_recovery_warnings += 1

    def add_event_source(self, event_sources):
        """
            Add EventSources used in extraction to Plaso storage
        Params:
            event_sources ([EventSource]): Event sources from extraction
        """
        for event_source in event_sources:
            self.parser_mediator.ProduceEventSource(event_source)

    def add_event_data_stream(self, event_data_streams):
        """
            Add data streams used in extraction to Plaso storage
        Params:
            event_data_streams ([EventDataStream]): Data streams from EventSources used in extraction
        :return:
        """
        for stream_event in event_data_streams:
            self.parser_mediator.ProduceEventDataStream(stream_event)

    def add_event(self, event_data, data_streams):
        parsers = []
        active_data_stream = None

        for event in event_data:
            if not active_data_stream:
                for stream in data_streams:
                    if stream.path_spec.location == event.spark_file_location:
                        active_data_stream = stream
                        break
                self.parser_mediator.ProduceEventDataStream(active_data_stream)

            else:
                if event.spark_file_location != active_data_stream.path_spec.location:
                    for stream in data_streams:
                        if stream.path_spec.location == event.spark_file_location:
                            active_data_stream = stream
                            break

                    self.parser_mediator.ProduceEventDataStream(active_data_stream)

            parsers.append(self.parser_mediator.ProduceEventData(event))

        return parsers

    def process_event_data(self):
        """
            Read EventData from Plaso storage and applies Plaso formating
            to extracted Event Data and stored formated Events into Plaso output
        """
        event_data = self.storage_writer.GetFirstWrittenEventData()

        while event_data:
            self.extraction_engine._event_data_timeliner.ProcessEventData(self.storage_writer, event_data)

            event_data = self.storage_writer.GetNextWrittenEventData()


        # Increase timeliner counter for Plaso output compatibility in pinfo tool
        for key, value in self.extraction_engine._event_data_timeliner.parsers_counter.items():
            parser_count = self.extraction_engine._parsers_counter.get(key, None)

            if parser_count:
                parser_count.number_of_events += value
                self.storage_writer.UpdateAttributeContainer(parser_count)
            else:
                parser_count = counts.ParserCount(name=key, number_of_events=value)
                self.extraction_engine._parsers_counter[key] = parser_count
                self.storage_writer.AddAttributeContainer(parser_count)

        # Increase parser mediator counter for Plaso output compatibility in pinfo tool
        for key, value in self.parser_mediator.parsers_counter.items():
            parser_count = self.extraction_engine._parsers_counter.get(key, None)

            if parser_count:
                parser_count.number_of_events += value
                self.storage_writer.UpdateAttributeContainer(parser_count)
            else:
                parser_count = counts.ParserCount(name=key, number_of_events=value)
                self.extraction_engine._parsers_counter[key] = parser_count
                self.storage_writer.AddAttributeContainer(parser_count)

    def get_parsers(self):
        """ Get all available parsers

        Returns:
            [FileObjectParser|FileEntryParser]: List of Plaso Parsers
        """
        return self.parsers

    def get_filter_expression(self):
        """
            Get filter expression representing available parser in extraction

        Returns:
            str: String representing filter of Parsers
        """
        return self.configuration.parser_filter_expression

    def get_nonsig_parsers(self):
        """
            Get non-signature parsers list
        Returns:
            [str]: Names of non signature parsers
        """
        return self.non_sig_parsers
