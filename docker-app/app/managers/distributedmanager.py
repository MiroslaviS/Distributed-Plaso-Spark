""" Component for managing Distributed File system HDFS """

from dfvfs.path.factory import Factory
from dfvfs.lib.definitions import TYPE_INDICATOR_HDFS
from dfvfs.resolver.resolver import Resolver
from managers.interface import StorageInterface
import os
from pyarrow import input_stream


class DistributedFileManager(StorageInterface):
    """ Manager for HDFS file system providing operations for this storage """
    def __init__(self):
        """ Initialize Distributed file system manager"""
        hdfs_filesystem_path = Factory.NewPathSpec(TYPE_INDICATOR_HDFS, location='namenode')
        self.hdfs_file_system = Resolver.OpenFileSystem(hdfs_filesystem_path)

    def get_filesystem(self):
        """ Get the current open file system
        Returns:
             HDFSFileSystem: Object representing HDFS file system
        """
        return self.hdfs_file_system

    def get_files(self):
        """ Get all files from the distributed file system
        Returns:
            [str]: List of file path in HDFS storage
        """
        entries = self.hdfs_file_system.ListFileSystem()
        files = self.hdfs_file_system.GetOnlyFiles(entries)

        return files

    def open_file(self, path):
        """ Open file specified by given HDFS path
         Params:
            path (str): path to HDFS file

         Returns:
            NativeFile: PyArrow HDFS file object stream
         """
        return self.hdfs_file_system.open_file(path)

    def save_file(self, path, data=None):
        """ Save the given data into specified path in HDFS
         Params:
            path (str): Path for file where the data will be stored
            data (Optional(bytes)): Data to store in given HDFS path
         """
        self.hdfs_file_system.create_file(path, data)

    def delete_file(self, path):
        """ Deletes file in HDFS storage
        Params:
            path (str): HDFS path to deleting file
        """
        self.hdfs_file_system.delete_file(path)

    def list_folder(self, folder):
        """ Get list of file in given folder
        Params:
            folder (str): Path to listing folder
        """
        return self.hdfs_file_system.ListFileSystem(path=folder)

    def create_folder(self, path):
        """ Create folder on given path in HDFS
        Params:
            path (str): Path for newly created folder
        """
        self.hdfs_file_system.create_folder(path)

    def delete_folder(self, path):
        """ Delete folder on given HDFS path
        Params:
            path (str): Path for deleting folder in HDFS
        """
        self.hdfs_file_system.delete_folder(path)

    def upload_to_hdfs(self, upload_folder):
        """ Upload files from upload folder into HDFS storage
        Params:
            upload_folder (str): Path to local storage folder with files
        Returns:
            [str]: List of paths to saved files in HDFS
        """
        hdfs_saved_files = []

        for root, dirs, files in os.walk(upload_folder):
            hdfs_folder = root.replace(upload_folder, "")
            if hdfs_folder == "":
                hdfs_folder = "/"

            for directory in dirs:
                hdfs_dir_path = os.path.join(hdfs_folder, directory)
                self.hdfs_file_system.create_folder(hdfs_dir_path)

            for file in files:
                hdfs_file_path = os.path.join(hdfs_folder, file)

                local_file_path = os.path.join(root, file)
                success, hdfs_path = self.save_file_to_hdfs(hdfs_file_path, local_file_path)

                if success:
                    hdfs_saved_files.append(hdfs_path)

        return hdfs_saved_files

    def save_file_to_hdfs(self, hdfs_path, file_path):
        """ Save local file to HDFS storage
        Params:
            hdfs_path (str): Path to create new file
            file_path (str): Local path to uploading file
        Return:
            (Bool, str): HDFS path to saved file
        """
        with input_stream(file_path) as f:
            self.hdfs_file_system.upload_file(hdfs_path, f)

        return True, hdfs_path

    def delete_hdfs_files(self):
        """ Deletes all files from HDFS storage
        Returns:
             [str]: List of deleted files in HDFS
        """
        content = self.hdfs_file_system.ListFileSystem()
        files = self.hdfs_file_system.GetOnlyFiles(content)
        deleted_content = list()

        for file in files:
            self.delete_file(file)
            deleted_content.append(file)

        folders = [entry.path for entry in content if entry.path not in files]

        for folder in folders:
            try:
                self.delete_folder(folder)
                deleted_content.append(folder)
            except FileNotFoundError:
                # Expect FileNotFoundError bcs of deleting all folders
                # need to have list of all deleted files/folders
                pass

        return deleted_content
