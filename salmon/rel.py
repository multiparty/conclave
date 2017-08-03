import salmon.utils as utils

class Column():

    def __init__(self, relName, name, idx, typeStr, collSets):

        self.relName = relName
        self.name = name
        self.idx = idx
        self.typeStr = typeStr
        self.collSets = collSets

    def getName(self):

        return self.name

    def getIdx(self):

        return self.idx

    def dbgStr(self):

        collSetStr = " ".join(sorted(["{" + ",".join([str(p) for p in collSet]) + "}" for collSet in self.collSets]))
        return self.getName() + " " + collSetStr

    def mergeCollSetsIn(self, otherCollSets):

        self.collSets = utils.mergeCollSets(self.collSets, otherCollSets)

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

    def dbgStr(self):

        colStr = ", ".join([col.dbgStr() for col in self.columns])
        return "{}([{}]) {}".format(self.name, colStr, self.storedWith)

    def __str__(self):

        colStr = ", ".join([str(col) for col in self.columns])
        return "{}([{}])".format(self.name, colStr)
