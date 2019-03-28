"""
Functions for working with collusion set annotations.

TODO: Turn this into a dedicated module for working with collusion sets.
"""
import copy
import os
import operator

from conclave.swift import SwiftData
from conclave.dataverse import DataverseData

import functools
import warnings


def merge_coll_sets(left: set, right: set):
    """
    Merge two collusion records if possible.
    :param left: collusion record
    :param right: collusion record
    :returns: all combinations of collusion sets from records

    >>> left = {1, 2}
    >>> right = {2, 3, 4}
    >>> actual = merge_coll_sets(left, right)
    >>> expected = {2}
    >>> actual == expected
    True

    >>> left = {1, 2}
    >>> right = set()
    >>> actual = merge_coll_sets(left, right)
    >>> expected = set()
    >>> actual == expected
    True
    """
    return left & right


def trust_set_from_columns(columns: list):
    """
    Returns combined trust for given columns.

    >>> class FakeCol:
    ...     def __init__(self, trust_set):
    ...         self.trust_set = trust_set
    >>> columns = [FakeCol({1, 2}), FakeCol({2})]
    >>> actual = trust_set_from_columns(columns)
    >>> expected = {2}
    >>> actual == expected
    True
    >>> columns = [FakeCol({1, 2}), FakeCol({2}), "dummy"]
    >>> actual = trust_set_from_columns(columns)
    >>> expected = {2}
    >>> actual == expected
    True
    """
    coll_sets = [copy.copy(col.trust_set) for col in columns if hasattr(col, "trust_set")]
    return functools.reduce(lambda set_a, set_b: merge_coll_sets(set_a, set_b), coll_sets)


def find(columns: list, col_name: str):
    """
    Retrieve column by name.
    :param columns: columns to search
    :param col_name: name of column to return
    :returns: column
    """
    try:
        return next(iter([col for col in columns if col.get_name() == col_name]))
    except StopIteration:
        print("column '{}' not found in {}".format(col_name, [c.get_name() for c in columns]))
        return None


def defCol(name: str, typ: str, *coll_sets):
    """
    Legacy utility method for simplifying trust sets.

    >>> actual = defCol("a", "INTEGER", [1], [2], [1, 2, 3])
    >>> expected = ("a", "INTEGER", {1, 2, 3})
    >>> actual == expected
    True

    >>> actual = defCol("a", "INTEGER", 1, 2, 3)
    >>> expected = ("a", "INTEGER", {1, 2, 3})
    >>> actual == expected
    True

    >>> actual = defCol("a", "INTEGER", 1)
    >>> expected = ("a", "INTEGER", {1})
    >>> actual == expected
    True
    """

    if not coll_sets:
        trust_set = set()
    else:
        first_set = coll_sets[0]
        trust_set = copy.copy({first_set} if isinstance(first_set, int) else set(first_set))
        for ts in coll_sets[1:]:
            if isinstance(ts, int):
                ts_set = {ts}
            else:
                warnings.warn("Use of lists for trust sets is deprecated")
                ts_set = set(ts)
            trust_set |= ts_set
    return name, typ, trust_set


def concatenate_data(data_dir, out_filename):
    """
    TODO: this is hacky as hell and does zero error checking, clean up after deadline
    """

    ret = []

    header = ''
    for filename in os.listdir(data_dir):

        if filename.endswith(".csv"):

            f = open("{0}/{1}".format(data_dir, filename))
            lines = f.read().split("\n")
            header = lines[0]
            ret.extend(lines[1:])
            f.close()
            os.remove("{0}/{1}".format(data_dir, filename))

    with open("{0}/{1}.csv".format(data_dir, out_filename), 'w') as out_file:
        # dummy header for codegen things
        cols = header
        out_file.write("\n".join([cols] + ret))


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
        if len(files) == 0:
            swift_data.get_all_data(container, data_dir)
            concatenate_data(data_dir, swift_cfg['data']['filename'])
        else:
            for file in files:
                swift_data.get_data(container, file, data_dir)

    swift_data.close_connection()


def post_swift_data(conclave_config):
    """
    Store locally held data on Swift.
    """

    swift_cfg = conclave_config.system_configs['swift'].dest
    data_dir = conclave_config.input_path
    container = swift_cfg['data']['container_name']

    swift_data = SwiftData(swift_cfg)

    all_files = {}

    for subdir, dirs, files in os.walk(data_dir):
        for file in files:
            if file[0] != '.':
                all_files[file] = os.path.getmtime("{0}/{1}".format(data_dir, file))

    # this is a hack to avoid writing all intermittent files to swift
    # it grabs the most recently modified file, which should be the output file,
    # since it is written to last
    output_file = max(all_files.items(), key=operator.itemgetter(1))[0]
    swift_data.put_data(container, output_file, data_dir)

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
