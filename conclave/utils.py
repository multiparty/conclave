"""
Functions for working with collusion set annotations.

TODO: Turn this into a dedicated module for working with collusion sets.
"""
import functools
import copy


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


def trust_set_from_columns(columns: list):
    """
    Returned
    """
    coll_sets = [col.trust_set if hasattr(col, "trust_set") else set() for col in columns]
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
