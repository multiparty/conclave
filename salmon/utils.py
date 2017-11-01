"""
Various utility functions used throughout the library.
"""

import functools
import copy

def mergeCollSets(left, right):
    """
    Merge two column sets if possible.
    :param left: ???
    :param right: ???
    :returns: set of ???
    """
    res = set()
    if not left:
        return copy.copy(right)
    elif not right:
        return copy.copy(left)
    for leftCollSet in left:
        for rightCollSet in right:
            res.add(leftCollSet | rightCollSet)
    return res

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
    """
    return (name, tpy, set([frozenset(collSet) for collSet in collSets]))
