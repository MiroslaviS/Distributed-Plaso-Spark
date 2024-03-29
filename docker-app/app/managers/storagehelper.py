""" Component for extracting content of archives/images """
from dfvfs.helpers import source_scanner
from dfvfs.resolver import context as dfvfs_context
import os
from dfvfs.lib import definitions
from dfvfs.lib import errors
from dfvfs.path import factory as path_spec_factory
from dfvfs.analyzer import analyzer as dfvfs_analyzer
import libarchive


class ArchiveImageHelper:
    """ Image/Archive helper for extracting content """
    def __init__(self, upload_folder, preprocessed_folder, logger):
        """ Initialize ArchiveImageHelper
        Params:
            upload_folder (str): Path to upload folder in local storage system
            preprocessed_folder (str): Path to folder with processed files
            logger (logging.Logger): Logger for debug
        """
        self.extracted_counter = 0
        self._source_scanner = source_scanner.SourceScanner()
        self._resolver_context = dfvfs_context.Context()
        self.upload_folder = upload_folder
        self.preprocessed_folder = preprocessed_folder
        self.logger = logger

    def scan_source(self, path):
        """ Determines the type of given file and start preprocessing if needed
        Params:
            path (str): path to scanning file
        Returns:
             (Bool, [str]): List of extracted files from scanned file and flag for deleting the extracting file
        """
        delete_file = False
        source_path_spec = path_spec_factory.Factory.NewPathSpec(
            definitions.TYPE_INDICATOR_OS, location=path)

        scanner_context = source_scanner.SourceScannerContext()
        scanner_context.AddScanNode(source_path_spec, None)
        extracted_files = []

        try:
            self._source_scanner.Scan(scanner_context)
        except (ValueError, errors.BackEndError) as exception:
            self.logger(f'Unable to scan source with error: {exception!s} with path: {path}. Skipping')
            return False, []

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
                        if extracted_path[-1] == '/':
                            os.makedirs(extracted_path, exist_ok=True)
                            continue
                        try:
                            with open(extracted_path, "w") as fsrc:
                                f.readpath(fsrc)
                        except:
                            continue

                        extracted_files.append(extracted_path)

                if extracted_files:
                    delete_file = True

            elif is_compressed:
                # decompress file
                # self.log("COMPRESSED FILE! " + compression_source_path_spec)
                saved_file = self._extract_compressed_file(compression_source_path_spec)
                delete_file = True
                extracted_files.append(saved_file)

        elif source_type == definitions.SOURCE_TYPE_STORAGE_MEDIA_IMAGE:
            return_code, export_image_path = self._scan_media_storage(source_path_spec)
            extracted_files.append(export_image_path)
            delete_file = True

        return delete_file, extracted_files

    def _scan_media_storage(self, source_path):
        """
            Use plaso command line tool for extracting images
        Params:
            source_path (str): Path to file for image export
        Returns:
            (int, str): Return code, path to exported image folder
        """
        import subprocess

        source_path_location = source_path.location
        export_image_path = self._create_folder_from_archive(source_path_location, move=False)

        logfile = "delete_me_" + str(os.getpid())

        export_return_code = subprocess.run(['python3.7', '-m', 'tools.image_export', '-w', export_image_path, source_path_location, "--partitions", "all", "--volumes", "all", "-q", "--logfile", logfile, "-u"], cwd='/plaso')
        return export_return_code, export_image_path

    def _ScanSourceForArchive(self, path_spec):
        """ Determines if a path specification references an archive file.
            Also check if the archive is not compressed.

        Args:
          path_spec (PathSpec): path specification of file.

        Returns:
            PathSpec|None: path specification of archive or None
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
        """ Extract compressed data from file according to compressed type
        Params:
            path_spec (PathSpec): Path specification of compressed file
        Returns:
             str: Path to decompressed file
        """
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
        """ Decompress xz file format
        Params:
            file_path (str): Path to compressed xz file
        Returns:
            str: Path to decompressed file
        """
        import lzma
        with lzma.open(file_path) as f:
            folder = self._create_folder_from_archive(file_path)
            data = f.read()
            saved_file = self._save_compressed_data(data, file_path, folder)

        return saved_file

    def _process_gz_compression(self, file_path):
        """ Decompress gz file format
        Params:
            file_path (str): Path to compressed gz file
        Returns:
            str: Path to decompressed file
        """
        import gzip
        with gzip.open(file_path) as f:
            folder = self._create_folder_from_archive(file_path)
            data = f.read()
            saved_file = self._save_compressed_data(data, file_path, folder)

        return saved_file

    def _process_bzip2_compression(self, file_path):
        """ Decompress bz2 file format
        Params:
            file_path (str): Path to compressed bz2 file
        Returns:
            str: Path to decompressed file
        """
        import bz2
        with bz2.open(file_path) as f:
            folder = self._create_folder_from_archive(file_path)
            data = f.read()
            saved_file = self._save_compressed_data(data, file_path, folder)

        return saved_file

    def _scan_for_compression(self, path_spec):
        """ Determines the type of compression for given file
        Params:
            path_spec (PathSpec): Path specification of possible compressed file
        Returns:
             (Bool, PathSpec): True if is compressed file, Path specification of file
        """
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

    def _create_folder_from_archive(self, archive_path, move=True):
        """ Creates folder to represent the extracted archive with files
        Params:
            archive_path (str): Path to archive file
            move (bool): Determines if the archive needs to be moved to preprocessed folder
        Returns:
             str: Path to created folder
        """
        if "." in archive_path:
            file_name = os.path.basename(archive_path).replace(".", "_") + "_extracted" + str(self.extracted_counter)

        else:
            file_name = os.path.basename(archive_path) + "_extracted" + str(self.extracted_counter)

        self.extracted_counter += 1
        archive_folder = os.path.dirname(archive_path)

        if move:
            archive_folder = archive_folder.replace(self.upload_folder, self.preprocessed_folder)

        folder_name = os.path.join(archive_folder, file_name)

        os.makedirs(folder_name, exist_ok=True)

        return folder_name

    def _save_compressed_data(self, data, file_path, folder_path):
        """ Determines encoding of compressed data and saved them to new file
        Params:
            data (bytes): Data inside compressed file
            file_path (str): Path to compressed file
            folder_path (str): Path to folder for new data
        Returns:
              str: Path to new file with decompressed data
        """
        filename = file_path.split('/')[-1].split('.')[0]
        file_path = os.path.join(folder_path, filename)
        import chardet

        encoding_result = chardet.detect(data)
        self.logger(encoding_result)

        if not encoding_result['encoding']:
            encoding = "utf-8"
        else:
            encoding = encoding_result['encoding']

        try:
            decoded_data = data.decode(encoding)
            with open(file_path, 'w') as f:
                f.write(decoded_data)

        except:
            self.logger("Unable to decode file: " + file_path)

        return file_path
