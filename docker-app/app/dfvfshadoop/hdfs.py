""" Component for HDFS control"""

import os.path
from pyarrow import fs
from pyarrow._fs import FileType


class Hdfs:
    """ HDFS controller utilizing all operations on HDFS storage"""
    def __init__(self):
        """ Initialize the HDFS controller """
        self.fs = None

    def open_filesystem(self, hdfs_uri, user='hadoop'):
        """ Creates a connection to HDFS storage system
        Params:
            hdfs_uri (str): Uri to HDFS namenode
            user (Optional(str)): User for HDFS access
        """
        self.fs = fs.HadoopFileSystem(hdfs_uri, 8020, user=user)

    def close_filesystem(self):
        """ Closes the HDFS connection """
        self.fs = None

    def get_filesystem(self):
        """ Get the current HDFS connection
        Returns:
            HadoopFileSystem: Connection to HDFS storage
        """
        return self.fs

    def exists(self, path):
        """ Check if path represent an existing file in HDFS
        Params:
            path (str): Path to inspecting HDFS file
        Returns:
            Bool: If given path is file or not
        """
        file = self.fs.get_file_info(path)
        return file.type != FileType.NotFound

    def info(self, path):
        """ Get file info from HDFS storage
        Params:
            path (str): Path to file in HDFS
        Returns:
            pyarrow.FileInfo|[pyarrow.FileInfo]: PyArrow file info or list of file informations
        """
        info = self.fs.get_file_info(path)

        return info

    def create_file_path(self, path):
        """ Creates HDFS path to given file path
        Params:
            path (str): Path to file in HDFS
        Returns:
            str: Path to HDFS with namenode url
        """
        return f"hdfs://namenode{path}"

    def basenamePath(self, path):
        """ Get basename path for HDFS file
        Params:
            path (str): Path to HDFS file
        Returns:
             str: Basename of given HDFS file
        """
        basename = os.path.basename(path)
        if basename == '':
            basename = '/'

        return basename

    def dirnamePath(self, path):
        """ Get directory name of HDFS path
        Params:
            path (str): Path to HDFS file
        Returns:
             str: Directory name from given HDFS file
        """
        dirname = os.path.dirname(path)

        return dirname

    def list_files(self, path):
        """ Recursively list files from folder
        Params:
            path (str): Path to HDFS folder
        Returns:
            [pyarrow.NativeFile]: List of HDFS files in directory
        """
        files = self.fs.get_file_info(fs.FileSelector(path, recursive=True))

        return files

    def get_only_files(self, files):
        """ Filters only file type from files
        Params:
            files ([NativeFile]): List of HDFS files
        Returns:
            [str]: List of path to HDFS files
        """
        only_files = [file.path for file in files if file.type == FileType.File]

        return only_files

    def open_inputstream(self, path):
        """ Opens HDFS file for sequential reading
        Params:
            path (str): Path to HDFS folder
        Returns:
            pyarrow.NativeFile: Stream of specified HDFS file
        """
        file = self.fs.open_input_file(path)

        return file

    def create_file(self, path, data):
        """ Creates file in HDFS storage
        Params:
            path (str): Path to HDFS folder
            data (bytes): Data to be stored in HDFS
        """
        data_encoded = data.encode(encoding='UTF-8')
        with self.fs.open_output_stream(path) as hdfs_file:
            hdfs_file.write(data_encoded)
            hdfs_file.flush()

    def upload_file(self, path, upload_stream):
        """ Opens newly created file HDFS and upload data stream
            from given data stream
        Params:
            path (str): Path to HDFS file
            upload_stream (bytes): Data stream of uploading file
        """
        with self.fs.open_output_stream(path) as hdfs_file:
            hdfs_file.upload(upload_stream)
            hdfs_file.flush()

    def open_file(self, path):
        """ Opens file for sequential writing
        Params:
            path (str): Path to HDFS file
        Returns:
            pyarrow.NativeFile: HDFS file for writing
        """
        file = self.fs.open_output_stream(path)

        return file

    def create_folder(self, path):
        """ Recursively creates directory from given path
        Params:
            path (str): Path to HDFS folder
        """
        self.fs.create_dir(path, recursive=True)

    def delete_folder(self, path):
        """ Non recursively creates HDFS directory
        Params:
            path (str): Path to HDFS folder
        """
        self.fs.delete_dir(path)

    def delete_file(self, path):
        """ Delete HDFS file
        Params:
            path (str): Path to HDFS file
        """
        self.fs.delete_file(path)
