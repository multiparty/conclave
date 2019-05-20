"""
Functions for working with collusion set annotations.

TODO: Turn this into a dedicated module for working with collusion sets.
"""
import copy
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
