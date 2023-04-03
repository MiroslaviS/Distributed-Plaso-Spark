from abc import ABC, abstractmethod


class Formatter(ABC):
    registered_formatters = []

    @classmethod
    def register(cls, formatter):
        cls.registered_formatters.append(formatter)

    @abstractmethod
    def format(self, event):
        pass


