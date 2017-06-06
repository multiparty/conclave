import salmon.utils as utils

class Column():

    # For now we are assuming that a column has exactly one
    # collusion set, however this will change in the future
    def __init__(self, relName, idx, typeStr, collusionSet):

        self.relName = relName
        self.idx = idx
        self.typeStr = typeStr
        self.collusionSet = collusionSet

    def getName(self):

        return self.relName + "_" + str(self.idx)

    # Update collusion sets. For now this will just overwrite
    # the existing collusion set. This behavior will change
    # once a column has multiple collusion sets
    def updateCollSetWith(self, collusionSet):

        self.collusionSet = collusionSet

    def __str__(self):

        return self.getName()


class Relation():

    def __init__(self, name, columns, storedWith):

        self.name = name
        self.columns = columns
        self.storedWith = storedWith

    def rename(self, newName):

        self.name = newName
        for col in self.columns:
            col.relName = newName

    def isShared(self):

        return len(self.storedWith) > 1

    # Returns the union of the collusion sets of all columns
    def getCombinedCollusionSet(self):

        return utils.collusionSetUnion(self.columns)

    # Makes sure column indexes are same as the columns' positions
    # in the list. Call this after inserting new columns or otherwise
    # changing their order
    def updateColumnIndexes(self):

        for idx, col in enumerate(self.columns):
            col.idx = idx

    def updateColumns(self):

        self.updateColumnIndexes()
        for col in self.columns:
            col.relName = self.name

    def __str__(self):

        colStr = ", ".join([str(col) for col in self.columns])
        return "{}([{}])".format(self.name, colStr)
