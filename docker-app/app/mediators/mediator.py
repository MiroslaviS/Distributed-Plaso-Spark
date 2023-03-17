
from plaso.parsers import mediator


class ParserMediator:
    def __init__(self):
        self.event_queue = list()
        self.warning_queue = list()
        self.recovery_queue = list()
        self._cached_parser_chain = None
        self._parser_chain_components = list()

    def PopFromParserChain(self):
        """Removes the last added parser or parser plugin from the parser chain."""
        self._cached_parser_chain = None
        self._parser_chain_components.pop()

    def ProduceEventWithEventData(self, event, event_data):
        self.event_queue.append((event, event_data))

    def ProduceExtractionWarning(self, message, path_spec=None):
        self.warning_queue.append(message)

    def ProduceRecoveryWarning(self, message, path_spec=None):
        self.recovery_queue.append(message)

    def AppendToParserChain(self, name):
        """Adds a parser or parser plugin to the parser chain.

        Args:
          name (str): name of a parser or parser plugin.
        """
        self._cached_parser_chain = None
        self._parser_chain_components.append(name)
