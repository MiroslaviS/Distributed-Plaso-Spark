from dfvfshadoop.definitions import TYPE_INDICATOR_HDFS

from dfdatetime import posix_time as dfdatetime_posix_time

from dfvfs.vfs.directory import Directory
from dfvfs.vfs.file_entry import FileEntry
from dfvfs.lib.errors import BackEndError
from pyarrow._fs import FileType
from dfvfs.lib import definitions
from dfvfs.vfs.attribute import Attribute, StatAttribute
from dfvfs.path.hdfs_path_specification import HDFSPathSpec


class HDFSDirectory(Directory):
    def _EntriesGenerator(self):
        location = getattr(self.path_spec, 'location', None)

        if location and getattr(self._file_system, 'hdfs', None):
            try:
                for directory_entry in self._file_system.hdfs.list_files(location):
                    yield HDFSPathSpec(location=directory_entry)
            except Exception as e:
                raise BackEndError(f"Unable to list directory {location} with errors: {e}")


class HDFSFileEntry(FileEntry):
    TYPE_INDICATOR = TYPE_INDICATOR_HDFS

    def __init__(self, resolver_context, file_system, path_spec, is_root=False):
        location = getattr(path_spec, 'location', None)

        stat_info = None

        if location and getattr(file_system, 'hdfs', None):
            try:
                stat_info = file_system.hdfs.info(location)
            except Exception as e:
                raise BackEndError(f"Unable to retrieve {location} with error {e}")

        super(HDFSFileEntry, self).__init__(resolver_context, file_system, path_spec, is_root=is_root, is_virtual=False)

        self._name = None
        self._stat_info = stat_info
        self._location = location

        if stat_info:
            if stat_info.type == FileType.File:
                self.entry_type = definitions.FILE_ENTRY_TYPE_FILE
            elif stat_info.type == FileType.Directory:
                self.entry_type = definitions.FILE_ENTRY_TYPE_DIRECTORY

    def _GetDirectory(self):
        if self.entry_type != definitions.FILE_ENTRY_TYPE_DIRECTORY:
            return None

        return HDFSDirectory(self._file_system, self.path_spec)

    def _GetLink(self):
        if self._link is None:
            self._link = ''

            if not self.IsLink():
                return self._link

            # symlinks in HDFS are still not supported currently,
            # see[OPEN] https://issues.apache.org/jira/browse/HADOOP-10019

        return self._link

    def _GetStatAttribute(self):
        stat_attribute = StatAttribute()

        if self._stat_info:
            stat_attribute.size = self._stat_info.size

        stat_attribute.type = self.entry_type

        return stat_attribute

    def _GetSubFileEntries(self):
        if self._directory is None:
            self._directory = self._GetDirectory()

        if self._directory:
            for path_spec in self._directory.entries:
                yield HDFSFileEntry(self._resolver_context, self._file_system, path_spec)

    @property
    def access_time(self):
        if self._stat_info is None:
            return None

        posix_time = None
        timestamp = int(self._stat_info.mtime_ns)

        if timestamp:
            try:
                posix_time = dfdatetime_posix_time.PosixTimeInNanoseconds(timestamp=timestamp)
            except:
                return None

        return posix_time

    @property
    def change_time(self):
        # Change time is not available in HDFS
        return None

    @property
    def creation_time(self):
        # Creation time not available in HDFS
        return None

    @property
    def modification_time(self):
        return None

    @property
    def name(self):
        if self._name is None:
            if self._location is not None:
                self._name = self._file_system.BasenamePath(self._location)

        return self._name

    @property
    def size(self):
        return self._stat_info.size

    def GetLinkedFileEntry(self):
        link = self._GetLink()
        if not link:
            return None

        path_spec = HDFSPathSpec(location=link)
        return HDFSFileEntry(self._resolver_context, self._file_system, path_spec)

    def GetParentFileEntry(self):
        if self._location is None:
            return None

        parent_location = self._file_system.DirnamePath(self._location)
        if parent_location is None:
            return None

        if parent_location == '':
            return self._file_system.PATH_SEPARATOR

        path_spec = HDFSPathSpec(location=parent_location)
        return HDFSFileEntry(self._resolver_context, self._file_system, path_spec)

