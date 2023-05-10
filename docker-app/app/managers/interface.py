""" Interface for storage manager"""

from abc import ABC, abstractmethod


class StorageInterface(ABC):
    @abstractmethod
    def save_file(self, file):
        pass

    @abstractmethod
    def delete_file(self, file):
        pass

    @abstractmethod
    def list_folder(self, folder):
        pass

    @abstractmethod
    def create_folder(self, path):
        pass

    @abstractmethod
    def delete_folder(self, path):
        pass
