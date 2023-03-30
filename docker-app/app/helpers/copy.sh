cat /app/plaso_test/definitions.py >> /usr/local/lib/python3.7/dist-packages/dfvfs/lib/definitions.py

cp /app/plaso_test/hdfs_file_entry.py /usr/local/lib/python3.7/dist-packages/dfvfs/path/
cp /app/plaso_test/hdfs_file_system.py /usr/local/lib/python3.7/dist-packages/dfvfs/path/
cp /app/plaso_test/hdfs_path_specification.py /usr/local/lib/python3.7/dist-packages/dfvfs/path/
cp /app/plaso_test/hdfs_file_io.py /usr/local/lib/python3.7/dist-packages/dfvfs/file_io/
cp /app/plaso_test/hdfs_resolver_helper.py /usr/local/lib/python3.7/dist-packages/dfvfs/resolver/
cp /app/plaso_test/hdfs.py /usr/local/lib/python3.7/dist-packages/dfvfs/helpers/

echo "from dfvfs.path import hdfs_path_specification" >> /usr/local/lib/python3.7/dist-packages/dfvfs/path/__init__.py
echo "from dfvfs.resolver import hdfs_resolver_helper" >> /usr/local/lib/python3.7/dist-packages/dfvfs/resolver/__init__.py