""" dfvfs implementation of HDFS file specification
    for detailed explanation
    see https://dfvfs.readthedocs.io/en/latest/sources/developer/Adding-new-type.html
"""

from dfvfs.path.location_path_spec import LocationPathSpec
from dfvfs.lib.definitions import TYPE_INDICATOR_HDFS
from dfvfs.path import factory


class HDFSPathSpec(LocationPathSpec):
    TYPE_INDICATOR = TYPE_INDICATOR_HDFS
    _IS_SYSTEM_LEVEL = True

    def __init__(self, location=None, **kwargs):
        if not location:
            raise ValueError('Missing location value.')

        parent = None
        if 'parent' in kwargs:
            parent = kwargs['parent']
            del kwargs['parent']

        if parent:
            raise ValueError('Parent value set.')

        super(HDFSPathSpec, self).__init__(location=location, parent=parent, **kwargs)


factory.Factory.RegisterPathSpec(HDFSPathSpec)
