"""
Data structures to represent relations (i.e., data sets).
"""

import salmon.utils as utils

class Column():
    """
    Column data structure.
    """

    def __init__(self, relName, name, idx, typeStr, collSets):
        """Initialize object."""
        self.relName = relName
        self.name = name
        self.idx = idx
        self.typeStr = typeStr
        self.collSets = collSets

    def getName(self):
        """Return column name."""
        return self.name

    def getIdx(self):
        """Return column identifier."""
        return self.idx

    def dbgStr(self):
        """Return column identifier."""
        collSetStr = " ".join(sorted(["{" + ",".join([str(p) for p in collSet]) + "}" for collSet in self.collSets]))
        return self.getName() + " " + collSetStr

    def mergeCollSetsIn(self, otherCollSets):
        """Merge ??? into column."""
        self.collSets = utils.mergeCollSets(self.collSets, otherCollSets)

    def __str__(self):
        """Return string representation of column object."""
        return self.getName()


class Relation():
    """
    Relation data structure.
    """

    def __init__(self, name, columns, storedWith):
        """Initialize object."""
        self.name = name
        self.columns = columns
        self.storedWith = storedWith

    def rename(self, newName):
        """Rename relation."""
        self.name = newName
        for col in self.columns:
            col.relName = newName

    def isShared(self):
        """Determine if this relation is shared."""
        return len(self.storedWith) > 1

    def updateColumnIndexes(self):
        """
        Makes sure column indexes are same as the columns' positions
        in the list. Call this after inserting new columns or otherwise
        changing their order.
        """
        for idx, col in enumerate(self.columns):
            col.idx = idx

    def updateColumns(self):
        """Update relation name in relation column objects."""
        self.updateColumnIndexes()
        for col in self.columns:
            col.relName = self.name

    def dbgStr(self):
        """Return extended string representation for debugging."""
        colStr = ", ".join([col.dbgStr() for col in self.columns])
        return "{}([{}]) {}".format(self.name, colStr, self.storedWith)

    def __str__(self):
        """Return string representation of relation."""
        colStr = ", ".join([str(col) for col in self.columns])
        return "{}([{}])".format(self.name, colStr)
