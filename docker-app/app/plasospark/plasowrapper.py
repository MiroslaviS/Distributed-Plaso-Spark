
from plaso.cli.log2timeline_tool import Log2TimelineTool
from plaso.single_process.extraction_engine import SingleProcessEngine
from plaso.parsers import manager as parsers_manager


class PlasoWrapper:
    def __init__(self):
        self._artifact_definitions_path = '/app/plaso/share_artifacts/artifacts/'
        self.extraction_tool = Log2TimelineTool()
        self.extraction_engine = SingleProcessEngine()
        self.configuration = None
        self.parser_mediator = None
        self.parsers = None
        self.non_sig_parsers = None

        self.create_extraction_configs()

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
        self.parsers = parsers_manager.ParsersManager.GetParserObjects(parser_filter_expression=self.configuration.parser_filter_expression)

        self.non_sig_parsers = self.extraction_engine._extraction_worker._event_extractor._non_sigscan_parser_names

    def get_parsers(self):
        return self.parsers

    def get_filter_expression(self):
        return self.configuration.parser_filter_expression

    def get_nonsig_parsers(self):
        return self.non_sig_parsers
