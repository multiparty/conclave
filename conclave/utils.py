"""
Functions for working with collusion set annotations.

TODO: Turn this into a dedicated module for working with collusion sets.
"""
import functools
import copy
import os

from conclave.swift import SwiftData
from conclave.dataverse import DataverseData


def merge_coll_sets(left: set, right: set):
    """
    Merge two collusion records if possible.
    :param left: collusion record
    :param right: collusion record
    :returns: all combinations of collusion sets from records

    >>> left = {frozenset([1, 2]), frozenset([3, 4])}
    >>> right = {frozenset([5, 6]), frozenset([7])}
    >>> actual = merge_coll_sets(left, right)
    >>> expected = {frozenset({1, 2, 5, 6}), frozenset({1, 2, 7}), frozenset({3, 4, 5, 6}), frozenset({3, 4, 7})}
    >>> actual == expected
    True
    """

    if not left:
        return copy.copy(right)
    elif not right:
        return copy.copy(left)
    return {l | r for l in left for r in right}


def coll_sets_from_columns(columns: list):
    """
    Returned
    """
    coll_sets = [col.coll_sets if hasattr(col, "coll_sets") else set() for col in columns]
    return functools.reduce(lambda set_a, set_b: merge_coll_sets(set_a, set_b), coll_sets)


def find(columns: list, col_name: str):
    """
    Retrieve column by name.
    :param columns: ??? of columns
    :param col_name: name of column to return
    :returns: column
    :raises StopIteration: if column is not found
    """
    try:
        return next(iter([col for col in columns if col.get_name() == col_name]))
    except StopIteration:
        print("column '{}' not found in {}".format(col_name, [c.get_name() for c in columns]))
        return None


def defCol(name: str, typ: str, *coll_sets):
    """
    ???

    >>> actual = defCol("a", "INTEGER", [1], [2], [1, 2, 3])
    >>> expected = ('a', 'INTEGER', {frozenset({1, 2, 3}), frozenset({2}), frozenset({1})})
    >>> actual == expected

    """

    return name, typ, set([frozenset(coll_set) for coll_set in coll_sets])


def download_swift_data(conclave_config):
    """
    Download data from Swift to local filesystem.
    """

    swift_cfg = conclave_config.system_configs['swift'].source
    data_dir = conclave_config.input_path
    container = swift_cfg['data']['container_name']
    files = swift_cfg['data']['files']

    swift_data = SwiftData(swift_cfg)

    if files is not None:
        for file in files:
            swift_data.get_data(container, file, data_dir)

    swift_data.close_connection()


def post_swift_data(conclave_config):
    """
    Store locally held data on Swift.

    NOTE: if container_name doesn't exist, raises swiftclient.exceptions.ClientException

    Should check to see if container exists in the future, and create it if it doesn't exist.
    """
    input_swift_data = conclave_config.system_configs['swift'].source['data']['files']

    swift_cfg = conclave_config.system_configs['swift'].dest
    data_dir = conclave_config.input_path
    container = swift_cfg['data']['container_name']

    swift_data = SwiftData(swift_cfg)

    # this pushes all intermediate files to swift as well, will need some
    # way to identify only final output files in the future
    for subdir, dirs, files in os.walk(data_dir):
        for file in files:
            print(file)
            if file[0] != '.':
                if file not in input_swift_data:
                    swift_data.put_data(container, file, data_dir)

    swift_data.close_connection()


def download_dataverse_data(conclave_config):
    """
    Download files from Dataverse.

    TODO: close connection?
    """

    dv_conf = conclave_config.system_configs['dataverse']
    data_dir = conclave_config.input_path

    dv_data = DataverseData(dv_conf)
    dv_data.get_data(data_dir)


def post_dataverse_data(conclave_config):
    """
    Post output files to Dataverse.

    TODO: close connection?
    """

    input_dv_files = conclave_config.system_configs['dataverse']['source']['files']

    dv_conf = conclave_config.system_configs['dataverse']
    data_dir = conclave_config.input_path

    dv_data = DataverseData(dv_conf)

    for subdir, dirs, files in os.walk(data_dir):
        for file in files:
            print(file)
            if file[0] != '.':
                if file not in input_dv_files:
                    dv_data.put_data(data_dir, file)
