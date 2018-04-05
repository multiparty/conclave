"""
Helper class for get_data and put_data
Establishes swift connection and returns a connection object
"""

import configparser
from keystoneauth1.identity import v3
from keystoneauth1 import session
from swiftclient import client as swift_client


class SwiftHandler:
    """
    Initiates a connection to a project domain.
    """

    @staticmethod
    def get_scoped_session(os_auth_url, username, password, os_project_domain, os_project_name):
        """
        Create and return a scoped session.
        """

        password_auth = v3.Password(auth_url=os_auth_url,
                                    user_domain_name='default',
                                    username=username, password=password,
                                    project_domain_name=os_project_domain,
                                    project_name=os_project_name,
                                    unscoped=False)

        scoped_session = session.Session(auth=password_auth)
        return scoped_session

    def init_swift_connection(self, cfg_path):
        """
        Returns a Swift connection object
        Swift credentials should be stored as a .cfg file at <cfg_path>
        """

        config = configparser.ConfigParser()

        with open(cfg_path, 'r') as f:
            config.read_file(f)

        os_auth_url = config['AUTHORIZATION']['osAuthUrl']
        username = config['AUTHORIZATION']['username']
        password = config['AUTHORIZATION']['password']
        os_project_domain = config['PROJECT']['osProjectDomain']
        os_project_name = config['PROJECT']['osProjectName']

        scoped_session = self.get_scoped_session(
            os_auth_url, username, password, os_project_domain, os_project_name)

        swift_connection = swift_client.Connection(session=scoped_session)

        return swift_connection


class SwiftData:
    """
    Upload files to a swift container, download files from a container, and create a container.
    """

    def __init__(self, config_path):

        self.config_path = config_path
        self.swiftConnection = SwiftHandler().init_swift_connection(self.config_path)

    def create_container(self, container_name):
        """
        Create a container.
        """

        self.swiftConnection.put_container(container_name)
        print("Container {0} created.".format(container_name))

    def get_data(self, container_name, key, output_dir):
        """
        Retrieve data from an existing container.
        """

        response, contents = self.swiftConnection.get_object(container_name, key)

        with open("{0}/{1}".format(output_dir, key), 'wb') as out_file:
            out_file.write(contents)
        print("Wrote object {0} to {1}.".format(key, output_dir))

    def put_data(self, container_name, file_path, key):
        """
        Put data into an existing container.
        """

        c = open(file_path).read()

        self.swiftConnection.put_object(container_name, key , c, content_type='text/plain')
        print('Placed object {0} in container {1}'.format(key, container_name))