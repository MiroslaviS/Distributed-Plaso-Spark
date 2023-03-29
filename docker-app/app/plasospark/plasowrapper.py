
from plaso.cli.log2timeline_tool import Log2TimelineTool
from plaso.single_process.extraction_engine import SingleProcessEngine
from plaso.parsers import manager as parsers_manager
from plaso.engine import worker
from plaso.cli import log2timeline_tool
from plaso.storage import factory as storage_factory
from plaso.engine import engine

class PlasoWrapper(log2timeline_tool.Log2TimelineTool):
    def __init__(self):
        super(PlasoWrapper, self).__init__()

        self._artifact_definitions_path = '/app/plaso/share_artifacts/artifacts/'
        self.extraction_tool = Log2TimelineTool()
        self.extraction_engine = SingleProcessEngine()
        self.configuration = None
        self.parser_mediator = None
        self.parsers = None
        self.non_sig_parsers = None
        self.storage_writer = None
        self.session = None

        self.storage_file_path = "/output.plaso"
        self.create_extraction_configs()
        self.create_plaso_start_containers()

    def create_extraction_configs(self):
        self.extraction_tool.ParseArguments(['--debug', '--single-process'])
        self.extraction_tool._expanded_parser_filter_expression = (self.extraction_tool._GetExpandedParserFilterExpression(
                                                                        self.extraction_engine.knowledge_base))
        self.configuration = self.extraction_tool._CreateExtractionProcessingConfiguration()
        self.parser_mediator = self.extraction_engine._CreateParserMediator(self.extraction_engine.knowledge_base,
                                                                            self.extraction_tool._resolver_context,
                                                                            self.configuration)
        self.extraction_engine._extraction_worker = worker.EventExtractionWorker(parser_filter_expression=self.configuration.parser_filter_expression)
        self.parsers = parsers_manager.ParsersManager.GetParserObjects(parser_filter_expression=self.configuration.parser_filter_expression)

        self.non_sig_parsers = self.extraction_engine._extraction_worker._event_extractor._non_sigscan_parser_names

        self.storage_writer = storage_factory.StorageFactory.CreateStorageWriter(self._storage_format)
        self.storage_writer.Open(path=self.storage_file_path)
        self.parser_mediator.SetStorageWriter(self.storage_writer)

    def create_plaso_start_containers(self):
        self.session = engine.BaseEngine.CreateSession()
        session_start = self.session.CreateSessionStart()

        self.storage_writer.AddAttributeContainer(session_start)
        enabled_parsers = self.extraction_tool._expanded_parser_filter_expression.split(',')
        session_configuration = self.extraction_tool._CreateExtractionSessionConfiguration(self.session, enabled_parsers)

        self.storage_writer.AddAttributeContainer(session_configuration)

        system_configuration = self.extraction_engine.knowledge_base.GetSystemConfigurationArtifact()
        self.storage_writer.AddAttributeContainer(system_configuration)

    def create_plaso_complete_containers(self):
        session_completed = self.session.CreateSessionCompletion()
        self.storage_writer.AddAttributeContainer(session_completed)

    def add_event_source(self, event_sources):
        for event_source in event_sources:
            self.parser_mediator.ProduceEventSource(event_source)

    def get_parsers(self):
        return self.parsers

    def get_filter_expression(self):
        return self.configuration.parser_filter_expression

    def get_nonsig_parsers(self):
        return self.non_sig_parsers
