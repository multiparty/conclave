import functools
import copy

def mergeCollSets(left, right):

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

    collSets = [col.collSets if hasattr(col, "collSets") else set() for col in columns]
    return functools.reduce(lambda setA, setB: mergeCollSets(setA, setB), collSets)

def find(columns, colName):
    
    return next(iter([col for col in columns if col.getName() == colName]))


def defCol(name, tpy, *collSets):

    return (name, tpy, set([frozenset(collSet) for collSet in collSets]))
