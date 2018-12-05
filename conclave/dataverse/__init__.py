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

        if self.dataverse_connection is None:
            print("Previous connection was closed. Reinitialize this object.")
            return self

        aliased_dv = self.dataverse_connection.get_dataverse(self.cfg["source"]['alias'])
        dataset = aliased_dv.get_dataset_by_doi(self.cfg['source']['doi'])

        files = dataset.get_files()
        expected_files = set(self.cfg["source"]["files"])
        retrieved_files = set()

        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        # TODO: might be a better way to do this
        for f in files:
            if f.name in expected_files:
                retrieved_files.add(f.name)
                download_url = f.download_url
                # TODO: throw exception here if it fails
                req = requests.get(download_url)

                print("Writing object {0} to {1}.".format(f.name, file_path))

                with open("{0}/{1}".format(file_path, f.name), 'wb') as out:
                    out.write(req.content)

        if expected_files != retrieved_files:
            print("Not all files retrieved. Ensure that filenames passed "
                  "correspond correctly to files stored on this Dataverse.")


