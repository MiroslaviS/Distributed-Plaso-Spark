import os.path

from pyarrow import fs
from pyarrow._fs import FileType


class Hdfs:
    def __init__(self):
        self.fs = None

    def open_filesystem(self, hdfs_uri, user='hadoop'):
        self.fs = fs.HadoopFileSystem(hdfs_uri, 8020, user=user)

    def close_filesystem(self):
        self.fs = None

    def get_filesystem(self):
        return self.fs

    def exists(self, path):
        file = self.fs.get_file_info(path)
        return file.type != FileType.NotFound

    def info(self, path):
        info = self.fs.get_file_info(path)

        return info

    def create_file_path(self, path):
        return f"hdfs://namenode{path}"

    def basenamePath(self, path):
        basename = os.path.basename(path)
        if basename == '':
            basename = '/'

        return basename

    def dirnamePath(self, path):
        dirname = os.path.dirname(path)

        return dirname

    def list_files(self, path):
        files = self.fs.get_file_info(fs.FileSelector(path, recursive=True))

        return files

    def get_only_files(self, files):
        only_files = [file.path for file in files if file.type == FileType.File]

        return only_files

    def open_inputstream(self, path):
        file = self.fs.open_input_file(path)

        return file

    def create_file(self, path, data):
        data_encoded = data.encode(encoding='UTF-8')
        with self.fs.open_output_stream(path) as hdfs_file:
            hdfs_file.write(data_encoded)
            hdfs_file.flush()

    def open_file(self, path):
        file = self.fs.open_output_stream(path)

        return file

    def create_folder(self, path):
        self.fs.create_dir(path, recursive=True)

    def delete_folder(self, path):
        self.fs.delete_dir(path)

    def delete_file(self, path):
        self.fs.delete_file(path)
