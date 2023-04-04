from abc import ABC, abstractmethod


class Formatter(ABC):
    registered_formatters = []

    @abstractmethod
    def format(self, event):
        pass


