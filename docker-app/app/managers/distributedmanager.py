
from dfvfshadoop.hdfs_path_specification import HDFSPathSpec
from dfvfs.path.factory import Factory
from dfvfshadoop.definitions import TYPE_INDICATOR_HDFS
from dfvfs.resolver.resolver import Resolver
from managers.interface import StorageInterface

from helpers.hdfs import Hdfs


class DistributedFileManager(StorageInterface):
    def __init__(self):
        hdfs_filesystem_path = Factory.NewPathSpec(TYPE_INDICATOR_HDFS, location='namenode')
        self.hdfs_file_system = Resolver.OpenFileSystem(hdfs_filesystem_path)

    def get_filesystem(self):
        return self.hdfs_file_system

    def get_files(self):
        entries = self.hdfs_file_system.ListFileSystem()
        files = self.hdfs_file_system.GetOnlyFiles(entries)

        return files

    def open_file(self, path):
        return self.hdfs_file_system.open_file(path)

    def save_file(self, path, data=None):
        self.hdfs_file_system.create_file(path, data)

    def delete_file(self, path):
        self.hdfs_file_system.delete_file(path)

    def list_folder(self, folder):
        return self.hdfs_file_system.ListFileSystem(path=folder)

    def create_folder(self, path):
        self.hdfs_file_system.create_folder(path)

    def delete_folder(self, path):
        self.hdfs_file_system.delete_folder(path)
