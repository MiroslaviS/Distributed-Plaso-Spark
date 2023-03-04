import os

import dfvfs.encoding.decoder
from dfvfs.credentials import manager as credentials_manager
from dfvfs.lib import definitions
from dfvfs.lib import errors
from dfvfs.helpers import source_scanner
from dfvfs.helpers import windows_path_resolver
from dfvfs.path import factory as path_spec_factory
from dfvfs.resolver import resolver
from dfvfs.volume import factory as volume_system_factory
from dfvfs.helpers import source_scanner
from dfvfs.analyzer import analyzer as dfvfs_analyzer
from dfvfs.resolver import context as dfvfs_context
from dfvfs.compression import xz_decompressor
import zipfile
import libarchive

class StorageManager:
    def __init__(self, storage_folder, proccessed_folder, logger):
        self.upload_folder = storage_folder
        self.preprocessed_folder = proccessed_folder

        self._source_scanner = source_scanner.SourceScanner()
        self._resolver_context = dfvfs_context.Context()

        self._create_internal_folders()

        self.log = logger.warning

    def _create_internal_folders(self):
        os.makedirs(self.upload_folder, exist_ok=True)
        os.makedirs(self.preprocessed_folder, exist_ok=True)

    @staticmethod
    def delete_file(filename):
        os.remove(filename)

    def scanUploadedFiles(self, upload_paths):
        while upload_paths:
            entry = upload_paths.pop(0)
            if os.path.isdir(entry):
                dir_files = self._list_dir(entry)
                upload_paths.extend(dir_files)
                continue

            delete_file, exported_files = self._scanSource(entry)


            upload_paths.extend(exported_files)

            if delete_file:
                self.delete_file(entry)
            elif self.upload_folder in entry:
                # Move file to upload_ready folder
                # Move only files from upload folder (self.upload_folder/)
                # Into preprocessed folder
                self._move_file(entry)

        self.app.logger.info("Preprocessing file ended")

        # Delete log file from image_export
        os.remove("delete_me_" + str(os.getpid()))

    def _list_dir(self, path):
        files = os.listdir(path)
        list_files = []

        for file in files:
            file_path = os.path.join(path, file)
            list_files.append(file_path)

        return list_files

    def _move_file(self, file_path):
        filename = file_path.split('/')[-1]
        destination = os.path.join(self.preprocessed_folder, filename)

        os.rename(file_path, destination)

    def _scanSource(self, path):
        delete_file = False
        source_path_spec = path_spec_factory.Factory.NewPathSpec(
            definitions.TYPE_INDICATOR_OS, location=path)

        scanner_context = source_scanner.SourceScannerContext()
        scanner_context.AddScanNode(source_path_spec, None)
        extracted_files = []

        try:
            self._source_scanner.Scan(scanner_context)
        except (ValueError, errors.BackEndError) as exception:
            raise errors.ScannerError(
                f'Unable to scan source with error: {exception!s}')

        source_type = scanner_context.source_type
        if source_type == definitions.SOURCE_TYPE_FILE:
            # File entry is source type of file
            # Can be raw file or archive type
            # Determine if file is also an archive
            is_compressed, compression_source_path_spec = self._scan_for_compression(source_path_spec)
            is_archive = self._ScanSourceForArchive(source_path_spec)

            if is_archive:
                with libarchive.Archive(path) as f:
                    archive_folder = self._create_folder_from_archive(path)
                    for entry in f:
                        extracted_path = os.path.join(archive_folder, entry.pathname)
                        with open(extracted_path, "w") as fsrc:
                            f.readpath(fsrc)

                        extracted_files.append(extracted_path)

                if extracted_files:
                    delete_file = True

            elif is_compressed:
                # decompress file
                saved_file = self._extract_compressed_file(compression_source_path_spec)
                delete_file = True
                extracted_files.append(saved_file)

        elif source_type == definitions.SOURCE_TYPE_STORAGE_MEDIA_IMAGE:
            return_code, export_image_path = self._scan_media_storage(source_path_spec)
            extracted_files.append(export_image_path)
            delete_file = True

        return delete_file, extracted_files

    def _ScanSourceForArchive(self, path_spec):
        """Determines if a path specification references an archive file.
  
        Args:
          path_spec (dfvfs.PathSpec): path specification of the data stream.
  
        Returns:
          dfvfs.PathSpec: path specification of the archive file or None if not
              an archive file.
        """
        try:
            type_indicators = (
                dfvfs_analyzer.Analyzer.GetCompressedStreamTypeIndicators(
                    path_spec, resolver_context=self._resolver_context))
        except IOError:
            type_indicators = []

        if len(type_indicators) > 1:
            return False

        if type_indicators:
            type_indicator = type_indicators[0]
        else:
            type_indicator = None

        if type_indicator == definitions.TYPE_INDICATOR_BZIP2:
            path_spec = path_spec_factory.Factory.NewPathSpec(
                definitions.TYPE_INDICATOR_COMPRESSED_STREAM,
                compression_method=definitions.COMPRESSION_METHOD_BZIP2,
                parent=path_spec)

        elif type_indicator == definitions.TYPE_INDICATOR_GZIP:
            path_spec = path_spec_factory.Factory.NewPathSpec(
                definitions.TYPE_INDICATOR_GZIP, parent=path_spec)

        elif type_indicator == definitions.TYPE_INDICATOR_XZ:
            path_spec = path_spec_factory.Factory.NewPathSpec(
                definitions.TYPE_INDICATOR_COMPRESSED_STREAM,
                compression_method=definitions.COMPRESSION_METHOD_XZ,
                parent=path_spec)

        try:
            type_indicators = dfvfs_analyzer.Analyzer.GetArchiveTypeIndicators(
                path_spec, resolver_context=self._resolver_context)
        except IOError:
            return None

        if len(type_indicators) != 1:
            return None

        return path_spec_factory.Factory.NewPathSpec(
            type_indicators[0], location='/', parent=path_spec)

    def _extract_compressed_file(self, path_spec):
        type_indicator = path_spec.type_indicator
        file_path = path_spec.parent.location

        if type_indicator == definitions.TYPE_INDICATOR_COMPRESSED_STREAM:
            compression_method = path_spec.compression_method
            if compression_method == definitions.COMPRESSION_METHOD_XZ:
                saved_file = self._process_xz_compression(file_path)
            elif compression_method == definitions.COMPRESSION_METHOD_BZIP2:
                saved_file = self._process_bzip2_compression(file_path)
        elif type_indicator == definitions.TYPE_INDICATOR_GZIP:
            saved_file = self._process_gz_compression(file_path)
        else:
            saved_file = None

        return saved_file

    def _process_xz_compression(self, file_path):
        import lzma
        with lzma.open(file_path) as f:
            folder = self._create_folder_from_archive(file_path)
            data = f.read()
            saved_file = self._save_compressed_data(data, file_path, folder)

        return saved_file

    def _process_gz_compression(self, file_path):
        import gzip
        with gzip.open(file_path) as f:
            folder = self._create_folder_from_archive(file_path)
            data = f.read()
            saved_file = self._save_compressed_data(data, file_path, folder)

        return saved_file

    def _process_bzip2_compression(self, file_path):
        import bz2
        with bz2.open(file_path) as f:
            folder = self._create_folder_from_archive(file_path)
            data = f.read()
            saved_file = self._save_compressed_data(data, file_path, folder)

        return saved_file

    def _create_folder_from_archive(self, archive_path):
        file_name = os.path.basename(archive_path).replace(".", "_")
        archive_folder = os.path.dirname(archive_path)
        archive_folder = archive_folder.replace(self.upload_folder, self.preprocessed_folder)

        folder_name = os.path.join(archive_folder, file_name)
        os.makedirs(folder_name, exist_ok=True)

        return folder_name

    def _save_compressed_data(self, data, file_path, folder_path):
        filename = file_path.split('/')[-1].split('.')[0]
        file_path = os.path.join(folder_path, filename)
        with open(file_path, 'w') as f:
            f.write(data.decode('utf-8'))

        return file_path

    def _scan_for_compression(self, path_spec):
        try:
            type_indicators = dfvfs_analyzer.Analyzer.GetCompressedStreamTypeIndicators(path_spec, self._resolver_context)
        except IOError:
            type_indicators = []

        if type_indicators:
            type_indicator = type_indicators[0]
        else:
            return False, None

        if type_indicator == definitions.TYPE_INDICATOR_BZIP2:
            path_spec = path_spec_factory.Factory.NewPathSpec(
                definitions.TYPE_INDICATOR_COMPRESSED_STREAM,
                compression_method=definitions.COMPRESSION_METHOD_BZIP2,
                parent=path_spec)

        elif type_indicator == definitions.TYPE_INDICATOR_GZIP:
            path_spec = path_spec_factory.Factory.NewPathSpec(
                definitions.TYPE_INDICATOR_GZIP, parent=path_spec)

        elif type_indicator == definitions.TYPE_INDICATOR_XZ:
            path_spec = path_spec_factory.Factory.NewPathSpec(
                definitions.TYPE_INDICATOR_COMPRESSED_STREAM,
                compression_method=definitions.COMPRESSION_METHOD_XZ,
                parent=path_spec)

        return True, path_spec

    def _scan_media_storage(self, source_path):
        """
            Use plaso command line tool for extracting images
        """
        import subprocess

        source_path_location = source_path.location
        export_image_path = self._create_folder_from_archive(source_path_location)

        logfile = "delete_me_" + str(os.getpid())

        export_return_code = subprocess.run(['image_export.py', '-w', export_image_path, source_path_location, "--partitions", "all", "--volumes", "all", "-q", "--logfile", logfile])
        return export_return_code, export_image_path