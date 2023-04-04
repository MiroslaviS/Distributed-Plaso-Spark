import os

from pyarrow import fs, input_stream
from managers.interface import StorageInterface
from managers.storagehelper import ArchiveImageHelper


class LocalStorageManager(StorageInterface):
    def __init__(self, app):
        self.log = app.logger.warning

        self.uploaded_files = []
        self.hdfs = fs.HadoopFileSystem("namenode", 8020)
        self.debug = True
        self.upload_folder = app.config['UPLOAD_FOLDER']
        self.preprocessed_folder = app.config['PREPROCESSED_FOLDER']

        self.archive_image_helper = ArchiveImageHelper(self.upload_folder, self.preprocessed_folder)
        self._create_internal_folders()

    def create_folder(self, path):
        pass

    def delete_folder(self, path):
        pass

    def save_files(self, files):
        for file in files:
            self.save_file(file)

    def save_file(self, file):
        filename = os.path.join(self.upload_folder, file.filename)
        file.save(filename)

        self.uploaded_files.append(filename)

        return filename

    def preprocess_files(self):
        self.log("Starting preprocess process, deleting saved files cache")
        processed_files = list()

        while self.uploaded_files:
            entry = self.uploaded_files.pop(0)
            if os.path.isdir(entry):
                dir_files = self.list_folder(entry)
                self.uploaded_files.extend(dir_files)
                continue

            delete_file, exported_files = self.archive_image_helper.scan_source(entry)

            self.uploaded_files.extend(exported_files)

            if delete_file:
                self.delete_file(entry)
            elif self.upload_folder in entry:
                # Move file to upload_ready folder
                # Move only files from upload folder (self.upload_folder/)
                # Into preprocessed folder
                processed_file = self.move_file(entry)
                processed_files.append(processed_file)

        self.uploaded_files = []
        self.log("Preprocess ended, all files ready for HDFS upload!")
        return processed_files

    def _create_internal_folders(self):
        os.makedirs(self.upload_folder, exist_ok=True)
        os.makedirs(self.preprocessed_folder, exist_ok=True)

    def delete_file(self, filename):
        os.remove(filename)

    def list_folder(self, path):
        files = os.listdir(path)
        list_files = []

        for file in files:
            file_path = os.path.join(path, file)
            list_files.append(file_path)

        return list_files

    def clear_hdfs_upload_folder(self):
        files = self.list_folder(self.preprocessed_folder)
        for file in files:
            self.delete_file(file)

    def move_file(self, file_path):
        filename = file_path.split('/')[-1]
        destination = os.path.join(self.preprocessed_folder, filename)

        os.rename(file_path, destination)

        return destination

