"""
Helper class for get_data and put_data
Establishes swift connection and returns a connection object
"""
import os

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

    def init_swift_connection(self, cfg):
        """
        Returns a Swift connection object
        """

        os_auth_url = cfg['auth']['osAuthUrl']
        username = cfg['auth']['username']
        password = cfg['auth']['password']
        os_project_domain = cfg['project']['osProjectDomain']
        os_project_name = cfg['project']['osProjectName']

        scoped_session = self.get_scoped_session(
            os_auth_url, username, password, os_project_domain, os_project_name)

        swift_connection = swift_client.Connection(session=scoped_session)

        return swift_connection


class SwiftData:
    """
    Upload files to a swift container, download files from a container, and create a container.
    """

    def __init__(self, cfg):

        self.cfg = cfg
        self.swift_connection = SwiftHandler().init_swift_connection(self.cfg)

    def create_container(self, container_name):
        """
        Create a container.
        """

        if self.swift_connection is None:
            print("Previous connection was closed. Reinitialize this object.")
            return self

        self.swift_connection.put_container(container_name)
        print("Container {0} created.".format(container_name))

    def get_data(self, container_name, key, file_path):
        """
        Retrieve data from an existing container.
        """

        if self.swift_connection is None:
            print("Previous connection was closed. Reinitialize this object.")
            return self

        response, contents = self.swift_connection.get_object(container_name, key)

        full_path = "{0}/{1}".format(file_path, key)

        os.makedirs(os.path.dirname(full_path), exist_ok=True)

        with open("{0}/{1}".format(file_path, key), 'wb') as out_file:
            out_file.write(contents)

        print("Wrote object {0} to {1}.".format(key, file_path))

    def get_all_data(self, container_name, file_path):

        if self.swift_connection is None:
            print("Previous connection was closed. Reinitialize this object.")
            return self

        for data in self.swift_connection.get_container(container_name)[1]:
            self.get_data(container_name, data['name'], file_path)

    def put_data(self, container_name, key, file_path):
        """
        Put data into an existing container.
        """

        if self.swift_connection is None:
            print("Previous connection was closed. Reinitialize this object.")
            return self

        # check if destination container exists, create it if not
        response, containers = self.swift_connection.get_account()
        if container_name in containers:
            pass
        else:
            self.create_container(container_name)

        c = open("{0}/{1}".format(file_path, key), encoding='UTF-8').read()

        self.swift_connection.put_object(container_name, key, c, content_type='text/plain')
        print('Placed object {0} in container {1}'.format(key, container_name))

    def close_connection(self):
        """
        Close a swift connection.
        """

        if self.swift_connection is not None:
            self.swift_connection.close()
            self.swift_connection = None

        return self
