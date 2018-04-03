"""
Helper class for GetData and PutData.
Establishes swift connection and returns a connection object
"""

import os
from keystoneauth1.identity import v3
from keystoneauth1 import session
from swiftclient import client as swift_client


class SwiftHandler:

    @staticmethod
    def _get_scoped_session(auth_url, username, password, project_domain, project_name):
        """
        Uses keystone authentication to create and return a scoped session
        """

        password_auth = v3.Password(auth_url=auth_url,
                                    user_domain_name='default',
                                    username=username,
                                    password=password,
                                    project_domain_name=project_domain,
                                    project_name=project_name,
                                    unscoped=False)

        scoped_session = session.Session(auth=password_auth)

        return scoped_session

    def init_swift_connection(self, config):
        """
        Initiates a Swift connection and returns a Swift connection object.
        """

        auth_url = config.auth_url
        username = config.username
        password = config.password
        project_domain_name = config.project_domain_name
        project_name = config.project_name

        scoped_session = self._get_scoped_session(auth_url, username, password, project_domain_name, project_name)
        swift_connection = swift_client.Connection(session=scoped_session)

        return swift_connection

    @staticmethod
    def delete_empty_dir(key):
        """
        Deletes the empty directory created by Swift in the parent directory.
        """

        directory_path = os.path.join(os.path.dirname(__file__), '../{}'.format(key))

        try:
            os.rmdir(directory_path)
            print("Temporary directory {} deleted".format(key))
        except:
            print("No temporary directory found")