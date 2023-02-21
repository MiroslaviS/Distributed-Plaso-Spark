import os

from managers.storagemanager import StorageManager
from multiprocessing import Process
from pyarrow import fs

class LocalStorageManager(StorageManager):
    def __init__(self, app):
        super(LocalStorageManager, self).__init__(app.config['UPLOAD_FOLDER'], app.config['PREPROCESSED_FOLDER'])

        self.app = app
        self.uploaded_files = []
        self.hdfs = fs.HadoopFileSystem("namenode", 8020)

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
        foo = self.hdfs.get_file_info(fs.FileSelector('/'))
        print("HDFS PATTH: ", foo)

        self.hdfs.create_dir("/new_dir")

        with self.hdfs.open_output_stream("/test") as stream:
            stream.write('Jezisko')

        with self.hdfs.open_input_file("/test") as f:
            result = f.readall()

        return result.decode("utf-8")

