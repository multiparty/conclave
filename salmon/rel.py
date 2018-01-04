"""
Data structures to represent relations (i.e., data sets).
"""
import salmon.utils as utils


class Column():
    """
    Column data structure.
    """

    def __init__(self, rel_name, name, idx, type_str, coll_sets):
        """Initialize object."""
        self.rel_name = rel_name
        self.name = name
        self.idx = idx  # Integer index of the column in the relation.
        self.type_str = type_str  # Currently can only be "INTEGER".
        self.coll_sets = coll_sets  # All sets of parties that can collude together to recover values in this column.

    def getName(self):
        """Return column name."""
        return self.name

    def getIdx(self):
        """Return column identifier."""
        return self.idx

    def dbgStr(self):
        """Return column identifier."""
        collSetStr = " ".join(sorted(["{" + ",".join([str(p) for p in collSet]) + "}" for collSet in self.coll_sets]))
        return self.getName() + " " + collSetStr

    def mergeCollSetsIn(self, otherCollSets):
        """Merge collusion sets into column."""
        self.coll_sets = utils.mergeCollSets(self.coll_sets, otherCollSets)

    def __str__(self):
        """Return string representation of column object."""
        return self.getName()


class Relation():
    """
    Relation data structure.
    """

    def __init__(self, name, columns, stored_with):
        """Initialize object."""
        self.name = name
        self.columns = columns
        self.stored_with = stored_with # Ownership of this data set. Does this refer to secret shares or open data?

    def rename(self, newName):
        """Rename relation."""
        self.name = newName
        for col in self.columns:
            col.rel_name = newName

    def is_shared(self):
        """Determine if this relation is shared."""
        return len(self.stored_with) > 1

    def updateColumnIndexes(self):
        """
        Makes sure column indexes are same as the columns' positions
        in the list. Call this after inserting new columns or otherwise
        changing their order.
        """
        for idx, col in enumerate(self.columns):
            col.idx = idx

    def update_columns(self):
        """Update relation name in relation column objects."""
        self.updateColumnIndexes()
        for col in self.columns:
            col.rel_name = self.name

    def dbgStr(self):
        """Return extended string representation for debugging."""
        colStr = ", ".join([col.dbgStr() for col in self.columns])
        return "{}([{}]) {}".format(self.name, colStr, self.stored_with)

    def __str__(self):
        """Return string representation of relation."""
        colStr = ", ".join([str(col) for col in self.columns])
        return "{}([{}])".format(self.name, colStr)
