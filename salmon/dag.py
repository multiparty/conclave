from salmon import rel

# TODO: abstract methods

class Node():

    def __init__(self, name):

        self.name = name
        self.children = set()
        self.parents = set()

    def debugStr(self):

        childrenStr = str([n.name for n in self.children])
        parentStr = str([n.name for n in self.parents])
        return self.name + " children: " + childrenStr + " parents: " + parentStr

    def __str__(self):

        return self.name


class OpNode(Node):

    def __init__(self, name, outRel):

        super(OpNode, self).__init__(name)
        self.outRel = outRel
        # By default we assume that the operator requires data
        # to cross party boundaries. Override this for operators
        # where this is not the case
        self.isLocal = False
        self.isMPC = False
        # Indicates whether we want to split this operation
        # into local step and mpc step. By default we don't
        self.canSplit = False

    # Indicates whether a node is at the boundary of MPC
    # that is if nodes above it are local (there are operators
    # such as aggregations that override this method since
    # other rules apply there)
    def isBoundary(self):

        return self.isMPC and not any([par.isMPC for par in self.parents])


    # By default operations are not reversible, i.e., given
    # the output of the operation we cannot learn the input
    # Note: for now we are only considering whether an entire relation
    # is reversible as opposed to column level reversibility
    def isReversible(self):

        return False

    def requiresMPC(self):

        return True

    def updateOpSpecificCols(self):
        # By default we don't need to do anything here
        return

    # For certain operators, we can propagate all collusion
    # sets from the output relation to the operator's input
    # relations (for example some projections).
    # Operators where that is the case can override this
    # method with the appropriate propagation rules.
    def backPropCollSets(self):

        return

    def makeOrphan(self):

        self.parents = set()

    def replaceParent(self, oldParent, newParent):

        self.parents.remove(oldParent)
        self.parents.add(newParent)

    def replaceChild(self, oldChild, newChild):

        self.children.remove(oldChild)
        self.children.add(newChild)


class UnaryOpNode(OpNode):

    def __init__(self, name, outRel, parent):

        super(UnaryOpNode, self).__init__(name, outRel)
        self.parent = parent
        if self.parent:
            self.parents.add(parent)

    def getInRel(self):

        return self.parent.outRel

    def requiresMPC(self):

        return self.getInRel().isShared() and not self.isLocal

    def makeOrphan(self):

        super(UnaryOpNode, self).makeOrphan()
        self.parent = None

    def replaceParent(self, oldParent, newParent):

        super(UnaryOpNode, self).replaceParent(oldParent, newParent)
        self.parent = newParent


class BinaryOpNode(OpNode):

    def __init__(self, name, outRel, leftParent, rightParent):

        super(BinaryOpNode, self).__init__(name, outRel)
        self.leftParent = leftParent
        self.rightParent = rightParent
        if self.leftParent:
            self.parents.add(leftParent)
        if self.rightParent:
            self.parents.add(rightParent)

    def getLeftInRel(self):

        return self.leftParent.outRel

    def getRightInRel(self):

        return self.rightParent.outRel

    def requiresMPC(self):

        leftStoredWith = self.getLeftInRel().storedWith
        rightStoredWith = self.getRightInRel().storedWith
        combined = leftStoredWith.union(rightStoredWith)
        return (len(combined) > 1) and not self.isLocal

    def makeOrphan(self):

        super(UnaryOpNode, self).makeOrphan()
        self.leftParent = None
        self.rightParent = None

    def replaceParent(self, oldParent, newParent):

        super(BinaryOpNode, self).replaceParent(oldParent, newParent)
        if self.leftParent == oldParent:
            self.leftParent = newParent
        elif self.rightParent == oldParent:
            self.rightParent = newParent


class NaryOpNode(OpNode):

    def __init__(self, name, outRel, parents):

        super(NaryOpNode, self).__init__(name, outRel)
        self.parents = parents

    def getInRels(self):

        # Returning a set here to emphasize that the order of
        # the returned relations is meaningless (since the parent-set
        # where we're getting the relations from isn't ordered)
        # If we want operators with multiple input relations where
        # the order matters, we do implement it as a separate class
        return set([parent.outRel for parent in self.parents])

    def requiresMPC(self):

        inCollSets = [inRel.storedWith for inRel in self.getInRels()]
        inRelsShared = len(set().union(*inCollSets)) > 1
        return inRelsShared and not self.isLocal


class Create(UnaryOpNode):

    def __init__(self, outRel):

        super(Create, self).__init__("create", outRel, None)
        # Input can be done by parties locally
        self.isLocal = True

    def requiresMPC(self):

        return False

    def __str__(self):

        colTypeStr = ", ".join([col.typeStr for col in self.outRel.columns])

        return "CREATE RELATION {} WITH COLUMNS ({})".format(
                self.outRel.name,
                colTypeStr
            )


class Store(UnaryOpNode):

    def __init__(self, outRel, parent):

        super(Store, self).__init__("store", outRel, parent)

    def isReversible(self):

        return True

    def backPropCollSets(self):

        inRelCols = self.getInRel().columns
        # TODO: oversimplifying here
        for col in inRelCols:
            col.updateCollSetWith(
                self.outRel.storedWith)

    def __str__(self):

        return "STORE RELATION {} INTO {} AS {}".format(
                self.getInRel().name,
                self.outRel.storedWith,
                self.outRel.name
            )

class Concat(NaryOpNode):

    def __init__(self, outRel, parents):

        super(Concat, self).__init__("concat", outRel, parents)

    def isReversible(self):

        return True

    def __str__(self):

        inRelStr = ", ".join([inRel.name for inRel in self.getInRels()])

        return "CONCAT{} [{}] AS {}".format(
                "MPC" if self.isMPC else "",
                inRelStr,
                self.outRel.name
            )


class Aggregate(UnaryOpNode):

    def __init__(self, outRel, parent, keyCol, aggCol, aggregator):

        super(Aggregate, self).__init__("aggregation", outRel, parent)
        self.keyCol = keyCol
        self.aggCol = aggCol
        self.aggregator = aggregator
        # We want to split aggregations
        self.canSplit = True

    def updateOpSpecificCols(self):

        self.keyCol = self.getInRel().columns[self.keyCol.idx]
        self.aggCol = self.getInRel().columns[self.aggCol.idx]

    def __str__(self):

        return "AGG{} [{}, {}] FROM ({}) GROUP BY [{}] AS {}".format(
                "MPC" if self.isMPC else "",
                self.aggCol.getName(),
                self.aggregator,
                self.getInRel().name,
                self.keyCol.getName(),
                self.outRel.name
            )


class Project(UnaryOpNode):

    def __init__(self, outRel, parent, selectedCols):

        super(Project, self).__init__("project", outRel, parent)
        # Projections can be done by parties locally
        self.isLocal = True
        self.selectedCols = selectedCols

    def updateOpSpecificCols(self):

        tempCols = self.getInRel().columns
        self.selectedCols = [tempCols[col.idx] for col in tempCols]

    def __str__(self):

        selectedColsStr = ", ".join([str(col) for col in self.selectedCols])

        return "PROJECT{} [{}] FROM ({}) AS {}".format(
                "MPC" if self.isMPC else "",
                selectedColsStr,
                self.getInRel().name,
                self.outRel.name
            )


class Multiply(UnaryOpNode):

    def __init__(self, outRel, parent, targetCol, operands):

        super(Multiply, self).__init__("multiply", outRel, parent)
        self.operands = operands
        self.targetCol = targetCol
        self.isLocal = True

    def updateOpSpecificCols(self):

        tempCols = self.getInRel().columns
        self.operands = [tempCols[col.idx] if isinstance(col, rel.Column) else col for col in tempCols]

    def isReversible(self):

        # A multiplication is reversible unless one of the operands is 0
        return all([op != 0 for op in self.operands])

    def backPropCollSets(self):

        if self.isReversible():
            inRelCols = self.getInRel().columns
            # TODO: oversimplifying here
            for col in inRelCols:
                col.updateCollSetWith(
                    self.outRel.storedWith)

    def __str__(self):

        operandStr = ", ".join([str(op) for op in self.operands])

        return "MUL{} [{}] FROM ({}) as {}".format(
                "MPC" if self.isMPC else "",
                operandStr,
                self.getInRel().name,
                self.outRel.name
            )


class Join(BinaryOpNode):

    def __init__(self, outRel, leftParent, rightParent, leftJoinCol, rightJoinCol):

        super(Join, self).__init__("join", outRel, leftParent, rightParent)
        self.leftJoinCol = leftJoinCol
        self.rightJoinCol = rightJoinCol

    def __str__(self):

        return "({}) JOIN{} ({}) ON {} AND {} AS {}".format(
                self.getLeftInRel().name,
                "MPC" if self.isMPC else "",
                self.getRightInRel().name,
                str(self.leftJoinCol),
                str(self.rightJoinCol),
                self.outRel.name
            )

    def updateOpSpecificCols(self):

        self.leftJoinCol = self.getLeftInRel().columns[self.leftJoinCol.idx]
        self.rightJoinCol = self.getRightInRel().columns[self.rightJoinCol.idx]


class Dag():

    def __init__(self, roots):

        self.roots = roots

    def _dfsVisit(self, node, visitor, visited):

        visitor(node)
        visited.add(node)
        for child in node.children:
            if child not in visited:
                self._dfsVisit(child, visitor, visited)

    def dfsVisit(self, visitor):

        visited = set()

        for root in self.roots:
            self._dfsVisit(root, visitor, visited)

        return visited

    def dfsPrint(self):

        self.dfsVisit(print)

    def getAllNodes(self):

        return self.dfsVisit(lambda node: node)

    # Note: not optimized at all but we're dealing with very small
    # graphs so performance shouldn't be a problem
    # Side-effects on all inputs other than node
    def _topSortVisit(self, node, marked, tempMarked, unmarked, ordered):

        if node in tempMarked:
            raise "Not a Dag!"

        if node not in marked:
            if node in unmarked:
                unmarked.remove(node)
            tempMarked.add(node)

            for otherNode in node.children:
                self._topSortVisit(
                    otherNode, marked, tempMarked, unmarked, ordered)

            marked.add(node)
            unmarked.add(node)
            tempMarked.remove(node)
            ordered.insert(0, node)

    def topSort(self):

        unmarked = self.getAllNodes()
        marked = set()
        tempMarked = set()
        ordered = []

        while unmarked:

            node = unmarked.pop()
            self._topSortVisit(node, marked, tempMarked, unmarked, ordered)

        return ordered

class OpDag(Dag):

    def __init__(self, roots):

        super(OpDag, self).__init__(roots)

    def __str__(self):

        order = self.topSort()
        return ",\n".join(str(node) for node in order)


def removeBetween(parent, child, other):

    assert(len(other.children) < 2)
    assert(len(other.parents) < 2)
    # only dealing with unary nodes for now
    assert(isinstance(other, UnaryOpNode))

    child.replaceParent(other, parent)
    child.updateOpSpecificCols()
    parent.replaceChild(other, child)

    other.makeOrphan()
    other.children = set()

def insertBetween(parent, child, other):

    # called with grandParent, topNode, toInsert
    assert(not other.children)
    assert(not other.parents)
    # only dealing with unary nodes for now
    assert(isinstance(other, UnaryOpNode))

    # Insert other below parent
    other.parents.add(parent)
    other.parent = parent
    parent.children.add(other)
    other.updateOpSpecificCols()

    # Remove child from parent
    if child:
        child.replaceParent(parent, other)
        if child in parent.children:
            parent.children.remove(child)
        child.updateOpSpecificCols()
        other.children.add(child)

