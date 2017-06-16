import functools

def mergeCollSets(left, right):

    res = set()
    for leftCollSet in left:
        for rightCollSet in right:
            res.add(leftCollSet | rightCollSet)
    return res

def collSetsFromColumns(columns):

    collSets = [col.collSets if hasattr(col, "collSets") else set() for col in columns] 
    return functools.reduce(lambda setA, setB: mergeCollSets(setA, setB), collSets)    

def find(columns, colName):
    
    return next(iter([col for col in columns if col.getName() == colName]))

def defCol(tpy, *collSets):

    return (tpy, set([frozenset(collSet) for collSet in collSets]))
