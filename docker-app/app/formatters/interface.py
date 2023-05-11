""" Interface for Spark formatter """

from abc import ABC, abstractmethod


class Formatter(ABC):
    @abstractmethod
    def format(self, event):
        """ Format given event with given implementation
            event (EventData): Extracted event data from file
        """
        pass


