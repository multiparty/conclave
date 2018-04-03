import os
import zipfile
import shutil
from io import BytesIO
from conclave.swift.handler import SwiftHandler


class GetData:
    """
    Retrieve data from Swift and store locally.
    """

    def __init__(self, cfg, write_to):

        self.cfg = cfg
        self.write_to = write_to
        self.swift_connection = None

    def _get_object(self, b_delete):
        """
        Returns an object associated with the specified k in the specified container
        Deletes the object after returning if specified
        """

        # TODO: whole get process is screwed up here.
        # TODO: need to figure out if 'container_name' is distinct from other fields already in cfg

        try:
            container_name = self.cfg['container_name']
            k = os.path.join('input', 'data')
            swift_data_object = self.swift_connection.get_object(container_name, k)

            if b_delete:
                self.swift_connection.delete_object(container_name, k)
                print('Deleted object with key {}'.format(k))

            return swift_data_object

        except Exception as exp:
            print(exp)

        return

    def get_data(self):
        """
        Gets the data from the Swift storage, zips and/or encodes it and sends it to the client
        """

        try:
            swift_handler = SwiftHandler()
            self.swift_connection = swift_handler.init_swift_connection(self.cfg)
            data_object = self._get_object(False)

        except Exception as err:
            print(err)
            return

        object_value = data_object[1]
        file_content = object_value
        file_bytes = BytesIO(file_content)

        zipfile_obj = zipfile.ZipFile(file_bytes, 'r', compression=zipfile.ZIP_DEFLATED)

        if not os.path.exists(self.write_to):
            os.makedirs(self.write_to)
        zipfile_obj.extractall(self.write_to)


class PutData:
    """
    Place data held locally on Swift.
    """

    def __init__(self, cfg, write_from):

        self.cfg = cfg
        self.write_from = write_from
        self.swift_connection = None

    def _put_object(self, container_name, key, value):
        """
        Creates an object with the given key and value and puts the object in the specified container
        """

        try:
            self.swift_connection.put_object(container_name, key, contents=value, content_type='text/plain')
            print('Object added with key {}'.format(key))

        except Exception as exp:
            print('Exception: {}'.format(exp))

    def store_data(self):
        """
        Creates an object of the file and stores it into the container as key-value object
        """

        shutil.make_archive('/tmp/ziparchive', 'zip', self.write_from)

        # TODO: might need to remove hardcoding here if /tmp/ has permissions issues
        try:
            with open('/tmp/ziparchive.zip', 'rb') as f:
                zipped_file_content = f.read()
        finally:
            os.remove('/tmp/ziparchive.zip')

        swift_handler = SwiftHandler()
        self.swift_connection = swift_handler.init_swift_connection(self.cfg)

        try:
            container_name = self.cfg['container_name']
            key = os.path.join('output', 'data')
            self._put_object(container_name, key, zipped_file_content)

        except Exception as err:
            print(err)

        finally:
            swift_handler.delete_empty_dir(key)