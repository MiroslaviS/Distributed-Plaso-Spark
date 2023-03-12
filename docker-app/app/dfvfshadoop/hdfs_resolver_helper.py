
from dfvfs.resolver_helpers.resolver_helper import ResolverHelper
from dfvfshadoop.definitions import TYPE_INDICATOR_HDFS
from dfvfshadoop.hdfs_file_io import HDFSFile
from dfvfshadoop.hdfs_file_system import HDFSFileSystem
from dfvfs.resolver_helpers import manager

class HDFSResolverHelper(ResolverHelper):
    TYPE_INDICATOR = TYPE_INDICATOR_HDFS

    def NewFileObject(self, resolver_context, path_spec):
        return HDFSFile(resolver_context, path_spec)

    def NewFileSystem(self, resolver_context, path_spec):
        return HDFSFileSystem(resolver_context, path_spec)


manager.ResolverHelperManager.RegisterHelper(HDFSResolverHelper())
