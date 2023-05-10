""" Custom ParserMediator class used in Spark jobs"""

from plaso.parsers import mediator as parsers_mediator


class ParserMediator(parsers_mediator.ParserMediator):
    """ Parser mediator component used in Spark jobs"""
    def __init__(self, mediator_data):
        """ Initialize ParserMediator
        Params:
            mediator_data (MediatorHolder): Data holding necessary data for creating Plaso ParserMediator
        """
        super(ParserMediator, self).__init__(mediator_data.knowledge_base,
                                             collection_filters_helper=mediator_data.collection_filter)

        self.mediator_data = mediator_data
        self.event_queue = list()
        self.warning_queue = list()
        self.recovery_queue = list()
        self._cached_parser_chain = None
        self._parser_chain_components = list()
        self._file_entry = None

        self.set_parser_details(mediator_data.configuration)

    def set_parser_details(self, configuration):
        """ Sets configurations for extraction

        Params:
            configuration (ProcessingConfiguration): Configuration for extraction process
        """
        self.SetExtractWinEvtResources(configuration.extraction.extract_winevt_resources)
        self.SetPreferredCodepage(configuration.preferred_codepage)
        self.SetPreferredLanguage(configuration.preferred_language)
        self.SetPreferredTimeZone(configuration.preferred_time_zone)
        self.SetTemporaryDirectory(configuration.temporary_directory)

    def GetFileEntry(self):
        """ Get the current file entry in extraction process """
        return self._file_entry

    def GetDisplayNameForPathSpec(self, path_spec):
        """ Gets display name (location) for currently processing file"""
        return path_spec.location

    def GetRelativePathForPathSpec(self, path_spec):
        """ Gets relative path (location) for currently processing file) """
        return path_spec.location

    def SetFileEntry(self, file_entry):
        """ Sets currently processing file entry """
        self._file_entry = file_entry

    def PopFromParserChain(self):
        """Removes the last added parser or parser plugin from the parser chain."""
        self._cached_parser_chain = None
        self._parser_chain_components.pop()

    def GetParserChain(self):
        """Retrieves the current parser chain.

        Returns:
          str: parser chain.
        """
        if not self._cached_parser_chain:
            self._cached_parser_chain = '/'.join(self._parser_chain_components)
        return self._cached_parser_chain

    def ProduceEventData(self, event_data):
        """ Save given event data from extraction to event queue
         Params:
            event_data (EventData): Extracted event from file
         """
        parser_name = self.GetParserChain()
        event_data.parser = parser_name
        event_data.spark_file_location = self._file_entry._location
        self.event_queue.append(event_data)

    def ProduceExtractionWarning(self, message, path_spec=None):
        """ Save extraction warning from extraction to warning queue
         Params:
            message (str): Warning message from extraction
            path_spec (HDFSPathSpec): HDFS file path specification
         """
        from plaso.containers import warnings

        parser_chain = self.GetParserChain()
        extraction_warning = warnings.ExtractionWarning(
            message=message, parser_chain=parser_chain, path_spec=path_spec
        )

        self.warning_queue.append(extraction_warning)

    def ProduceRecoveryWarning(self, message, path_spec=None):
        """ Save extraction recovery warning to recovery queue
         Params:
            message (str): Recovery warning message from extraction
            path_spec (HDFSPathSpec): HDFS file path specification
         """
        from plaso.containers import warnings

        parser_chain = self.GetParserChain()
        recovery_warning = warnings.RecoveryWarning(
            message=message, parser_chain=parser_chain, path_spec=path_spec
        )

        self.recovery_queue.append(recovery_warning)

    def AppendToParserChain(self, parser):
        """Adds a parser or parser plugin to the parser chain.

        Args:
          parser (str): name of a parser or parser plugin.
        """
        self._cached_parser_chain = None
        self._parser_chain_components.append(parser)

    def SampleStartTiming(self, parser_chain):
        """ Not needed for Spark Mediator holder just needs to override the method for extraction process """
        return

    def SampleStopTiming(self, parser_chain):
        """ Not needed for Spark Mediator holder just needs to override the method for extraction process """
        return