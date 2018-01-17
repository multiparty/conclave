"""
Data structures to represent relations (i.e., data sets).
"""
import salmon.utils as utils


class Column:
    """
    Column data structure.
    """

    def __init__(self, rel_name: str, name: str, idx: int, type_str: str, coll_sets: set):
        """Initialize object."""
        self.rel_name = rel_name
        self.name = name
        self.idx = idx  # Integer index of the column in the relation.
        self.type_str = type_str  # Currently can only be "INTEGER".
        self.coll_sets = coll_sets  # Record of all sets of parties that can collude together to recover values in this column.

    def get_name(self):
        """Return column name."""
        return self.name

    def get_idx(self):
        """Return column identifier."""
        return self.idx

    def dbg_str(self):
        """Return column identifier."""
        coll_set_str = \
            " ".join(sorted(["{" + ",".join([str(p) for p in coll_set]) + "}" for coll_set in self.coll_sets]))
        return self.get_name() + " " + coll_set_str

    def merge_coll_sets_in(self, other_coll_sets: set):
        """Merge collusion sets into column."""
        self.coll_sets = utils.merge_coll_sets(self.coll_sets, other_coll_sets)

    def __str__(self):
        """Return string representation of column object."""
        return self.get_name()


class Relation:
    """
    Relation data structure.
    """

    def __init__(self, name: str, columns: list, stored_with: set):
        """Initialize object."""
        self.name = name
        self.columns = columns
        self.stored_with = stored_with  # Ownership of this data set. Does this refer to secret shares or open data?

    def rename(self, new_name):
        """Rename relation."""
        self.name = new_name
        for col in self.columns:
            col.rel_name = new_name

    def is_shared(self):
        """Determine if this relation is shared."""
        return len(self.stored_with) > 1

    def update_column_indexes(self):
        """
        Makes sure column indexes are same as the columns' positions
        in the list. Call this after inserting new columns or otherwise
        changing their order.
        """
        for idx, col in enumerate(self.columns):
            col.idx = idx

    def update_columns(self):
        """Update relation name in relation column objects."""
        self.update_column_indexes()
        for col in self.columns:
            col.rel_name = self.name

    def dbg_str(self):
        """Return extended string representation for debugging."""
        col_str = ", ".join([col.dbg_str() for col in self.columns])
        return "{}([{}]) {}".format(self.name, col_str, self.stored_with)

    def __str__(self):
        """Return string representation of relation."""
        col_str = ", ".join([str(col) for col in self.columns])
        return "{}([{}])".format(self.name, col_str)
