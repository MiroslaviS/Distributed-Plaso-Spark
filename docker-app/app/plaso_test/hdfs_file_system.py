
from dfvfs.vfs.file_system import FileSystem
from dfvfshadoop.definitions import TYPE_INDICATOR_HDFS
from helpers.hdfs import Hdfs
from dfvfs.lib import errors
from dfvfs.path.hdfs_file_entry import HDFSFileEntry
from dfvfs.path.hdfs_path_specification import HDFSPathSpec


class HDFSFileSystem(FileSystem):
    TYPE_INDICATOR = TYPE_INDICATOR_HDFS
    PATH_SEPARATOR = '/'

    def __init__(self, resolver_context, path_spec):
        super(HDFSFileSystem, self).__init__(resolver_context, path_spec)
        self.hdfs = Hdfs()

    def _Close(self):
        self.hdfs.close_filesystem()

    def _Open(self, mode='rb'):
        if self._path_spec.HasParent():
            raise errors.PathSpecError("Unsupported HDFS path spec with parent.")

        location = getattr(self._path_spec, 'location', None)
        if location is None:
            raise ValueError('Missing location in path specification.')

        # TODO: Spravit open_filesystem genericky pomocou PathSpec s namenode lokaciou alebo
        # pomocou split nazvu suboru kde sa vytiahne -> `hdfs://namenode`
        self.hdfs.open_filesystem("namenode")

    def FileEntryExistsByPathSpec(self, path_spec):
        location = getattr(path_spec, 'location', None)

        if location is None:
            return False

        return self.hdfs.exists(location)

    def GetFileEntryByPathSpec(self, path_spec):
        if not self.FileEntryExistsByPathSpec(path_spec):
            return None

        return HDFSFileEntry(self._resolver_context, self, path_spec)

    def GetRootFileEntry(self):
        path_spec = HDFSPathSpec(location='/')

        return HDFSFileEntry(self._resolver_context, self, path_spec)

    def JoinPath(self, path_segments):
        path_segments = [segment.split(self.PATH_SEPARATOR) for segment in path_segments]

        # Flatten the sublists into one list.
        path_segments = [
            element for sublist in path_segments for element in sublist]

        # Remove empty path segments.
        path_segments = list(filter(None, path_segments))

        path = ''.join([
            self.PATH_SEPARATOR, self.PATH_SEPARATOR.join(path_segments)])

        return path

    def ListFileSystem(self, path="/"):
        return self.hdfs.list_files(path)

    def GetOnlyFiles(self, files):
        return self.hdfs.get_only_files(files)

    def BasenamePath(self, path):
        return self.hdfs.basenamePath(path)

    def DirnamePath(self, path):
        return self.hdfs.dirnamePath(path)

    def create_file(self, file, data=None):
        self.hdfs.create_file(file, data)

    def delete_file(self, file):
        self.hdfs.delete_file(file)

    def create_folder(self, folder):
        self.hdfs.create_folder(folder)

    def delete_folder(self, folder):
        self.hdfs.delete_folder(folder)

    def open_file(self, path):
        return self.hdfs.open_file(path)



