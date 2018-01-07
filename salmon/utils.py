"""
Functions for working with collusion set annotations.

TODO: Turn this into a dedicated module for working with collusion sets.
"""
import functools
import copy


def mergeCollSets(left, right):
    """
    Merge two collusion records if possible.
    :param left: collusion record
    :param right: collusion record
    :returns: all combinations of collusion sets from records
    
    >>> left = set([frozenset([1, 2]), frozenset([3, 4])])
    >>> right = set([frozenset([5, 6]), frozenset([7])])
    >>> actual = mergeCollSets(left, right)
    >>> expected = {frozenset({1, 2, 5, 6}), frozenset({1, 2, 7}), frozenset({3, 4, 5, 6}), frozenset({3, 4, 7})}
    >>> actual == expected
    True
    """
    if not left:
        return copy.copy(right)
    elif not right:
        return copy.copy(left)
    return {l | r for l in left for r in right}

def collSetsFromColumns(columns):
    """
    ???
    """
    collSets = [col.collSets if hasattr(col, "collSets") else set() for col in columns]
    return functools.reduce(lambda setA, setB: mergeCollSets(setA, setB), collSets)


def find(columns, colName):
    """
    Retrieve column by name.
    :param columns: ??? of columns
    :param colName: name of column to return
    :returns: column
    :raises StopIteration: if column is not found
    """
    try:
        return next(iter([col for col in columns if col.getName() == colName]))
    except StopIteration:
        print("column '{}' not found in {}".format(colName, [c.getName() for c in columns]))
        return None


def defCol(name, tpy, *collSets):
    """
    ???
    
    >>> actual = defCol("a", "INTEGER", [1], [2], [1, 2, 3])
    >>> expected = ('a', 'INTEGER', {frozenset({1, 2, 3}), frozenset({2}), frozenset({1})})
    >>> actual == expected
    True
    """
    return name, tpy, set([frozenset(collSet) for collSet in collSets])
