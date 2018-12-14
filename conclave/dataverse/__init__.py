import requests
import os

from dataverse import Connection


class DataverseHandler:
    """
    Initiates a connection to a given Dataverse
    """

    @staticmethod
    def get_connection(cfg):

        return Connection(cfg["auth"]["host"], cfg["auth"]["token"])


class DataverseData:
    """
    Download & Upload files from a Dataverse
    """

    def __init__(self, cfg):

        self.cfg = cfg
        self.dataverse_connection = DataverseHandler().get_connection(cfg)

    def get_data(self, file_path):
        """
        Retrieve a file from Dataverse.
        """

        if self.dataverse_connection is None:
            print("Previous connection was closed. Reinitialize this object.")
            return self

        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        # TODO: implement multiple file retrieval functionality
        expected_file = self.cfg["source"]["files"]
        file_found = False

        aliased_dv = self.dataverse_connection.get_dataverse(self.cfg["source"]['alias'])
        dataset = aliased_dv.get_dataset_by_doi(self.cfg['source']['doi'])
        files = dataset.get_files()

        for f in files:
            if f.name == expected_file:
                file_found = True
                download_url = f.download_url

                # TODO: handle download failure
                req = requests.get(download_url, auth=self.dataverse_connection.auth)

                with open("{0}/{1}".format(file_path, f.name), 'wb') as out:
                    out.write(req.content)

                print("Wrote object {0} to {1}.".format(f.name, file_path))
                break

        if not file_found:
            print("Could not locate file {}. "
                  "Check to make sure this file is stored on this Dataverse."
                  .format(expected_file))

    def put_data(self, file_path, file):
        """
        Push output file back to Dataverse.
        """

        if self.dataverse_connection is None:
            print("Previous connection was closed. Reinitialize this object.")
            return self

        content = open("{0}/{1}".format(file_path, file), 'r').read()

        """
        # TODO: make DV output dataset endpoint configurable
        """

        aliased_dv = self.dataverse_connection.get_dataverse(self.cfg['dest']['alias'])
        dataset = aliased_dv.create_dataset(
            file,
            'Output data for {}.'.format(file),
            self.cfg['dest']['author']
        )
        dataset.upload_file(file, content)
