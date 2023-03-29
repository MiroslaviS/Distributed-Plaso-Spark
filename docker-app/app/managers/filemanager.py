import os

from managers.storagemanager import StorageManager
from multiprocessing import Process
from pyarrow import fs, input_stream


class LocalStorageManager(StorageManager):
    def __init__(self, app):
        super(LocalStorageManager, self).__init__(app.config['UPLOAD_FOLDER'], app.config['PREPROCESSED_FOLDER'], app.logger)

        self.app = app
        self.uploaded_files = []
        self.hdfs = fs.HadoopFileSystem("namenode", 8020)
        self.debug = True

    def log(self, *strings):
        string = " ".join(strings)

        self.app.logger.warning(string)

    def save_files(self, files):
        for file in files:
            self.save_file(file)

        return self.uploaded_files

    def save_file(self, file):
        filename = os.path.join(self.upload_folder, file.filename)
        file.save(filename)

        self.uploaded_files.append(filename)

        return filename

    def preprocess_files(self):
        self.app.logger.info("Starting preprocess process, deleting saved files cache")
        Process(target=self.scanUploadedFiles, args=(self.uploaded_files,)).start()

        self.uploaded_files = []

    def upload_to_hdfs(self):
        hdfs_files = self._process_files_to_hdfs()

        return hdfs_files

    def save_file_to_hdfs(self, hdfs_path, file_path):
        with self.hdfs.open_output_stream(hdfs_path) as stream:
            with input_stream(file_path) as f:
                stream.upload(f)
                stream.flush()

        if self.debug:
            with self.hdfs.open_input_file(hdfs_path) as f:
                try:
                    self.log(f"File {hdfs_path} -- {f.size()}")
                except:
                    return False, None

        return True, hdfs_path

    def _process_files_to_hdfs(self):
        hdfs_saved_files = []

        for root, dirs, files in os.walk(self.preprocessed_folder):
            hdfs_folder = root.replace(self.preprocessed_folder, "")
            if hdfs_folder == "":
                hdfs_folder = "/"

            self.log("ROOT - " + root)
            self.log("HDFS FOLDER - " + hdfs_folder)

            for directory in dirs:
                self.log("CREATING DIR: " + directory)
                hdfs_dir_path = os.path.join(hdfs_folder, directory)
                self.hdfs.create_dir(hdfs_dir_path)

            for file in files:
                hdfs_file_path = os.path.join(hdfs_folder, file)
                self.log("HDFS FILE PATH: " + hdfs_file_path)

                local_file_path = os.path.join(root, file)
                self.log("LOCAL FILE PATH: " + local_file_path)
                success, hdfs_path = self.save_file_to_hdfs(hdfs_file_path, local_file_path)
                if success:
                    hdfs_saved_files.append(hdfs_path)

        return hdfs_saved_files
