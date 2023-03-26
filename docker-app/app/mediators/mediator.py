
from plaso.parsers import mediator

class ParserMediator:
    def __init__(self):
        self.event_queue = list()
        self.warning_queue = list()
        self.recovery_queue = list()
        self._cached_parser_chain = None
        self._parser_chain_components = list()
        self._file_entry = None

    def GetFileEntry(self):
        return self._file_entry

    def GetDisplayNameForPathSpec(self, path_spec):
        return "AAA"

    def GetRelativePathForPathSpec(self, path_spec):
        return "/jezis"

    def SetFileEntry(self, file_entry):
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

    def ProduceEventWithEventData(self, event, event_data):
        parser_name = self.GetParserChain()
        event_data.parser = parser_name
        self.event_queue.append(event_data)

    def ProduceExtractionWarning(self, message, path_spec=None):
        self.warning_queue.append(message)

    def ProduceRecoveryWarning(self, message, path_spec=None):
        self.recovery_queue.append(message)

    def AppendToParserChain(self, parser):
        """Adds a parser or parser plugin to the parser chain.

        Args:
          name (str): name of a parser or parser plugin.
        """
        self._cached_parser_chain = None
        self._parser_chain_components.append(parser.NAME)