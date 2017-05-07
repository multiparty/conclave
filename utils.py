import functools

def collusionSetUnion(columns):

    colSets = [col.collusionSet if hasattr(col, "collusionSet") else set() for col in columns] 
    return functools.reduce(lambda setA, setB: setA.union(setB), colSets)

def find(columns, colName):
    return next(iter([col for col in columns if col.getName() == colName]))
