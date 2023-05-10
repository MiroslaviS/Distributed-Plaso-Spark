
from dfvfs.file_io.file_io import FileIO
from dfvfs.helpers.hdfs import Hdfs
from dfvfs.lib.errors import PathSpecError, BackEndError
import os


class HDFSFile(FileIO):
    def __init__(self, resolver_context, path_spec):
        super(HDFSFile, self).__init__(resolver_context, path_spec)
        self._hdfs = Hdfs()
        self._file_object = None
        self._size = 0

    def _Close(self):
        self._is_open = False
        if self._file_object:
            self._file_object.close()

        self._file_object = None

    def _Open(self, mode='rb'):
        if self._path_spec is None:
            raise ValueError("Missing path specification.")

        if self._path_spec.HasParent():
            raise PathSpecError('Unsupported path specification with parent.')

        location = getattr(self._path_spec, 'location', None)

        if location is None:
            raise PathSpecError('Path specification missing location.')

        if mode != 'rb':
            raise ValueError("Unsuppoerted open mode for file.")

        self._hdfs.open_filesystem("namenode", "")

        try:
            file_object = self._hdfs.open_inputstream(location)
            self._file_object = file_object
            self._size = file_object.size()
        except Exception as e:
            raise BackEndError(f"Unable to open file {location} with error: {e}")


    def read(self, size=None):
        if not self._is_open:
            raise IOError('Not opened.')

        if size is None:
            size = self._size - self._file_object.tell()

        return self._file_object.read(size)

    def seek(self, offset, whence=os.SEEK_SET):
        if not self._is_open:
            raise IOError('Not opened.')

        if whence not in (os.SEEK_CUR, os.SEEK_END, os.SEEK_SET):
            raise IOError('Unsupported whence.')

        self._file_object.seek(offset, whence)

    def get_offset(self):
        if not self._is_open:
            raise IOError('Not opened.')

        return self._file_object.tell()

    def get_size(self):
        if not self._is_open:
            raise IOError('Not opened.')

        return self._file_object.size()
