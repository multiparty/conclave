from salmon import rel

class Node():

    def __init__(self, name):
        
        self.name = name
        self.children = set()
        self.parents = set()

    # side-effect on child
    def addChild(self, child):
        
        self.children.add(child)
        child.parents.add(self)

    # side-effect on parent
    def addParent(self, parent):
        
        self.parents.add(parents)
        parent.children.add(self)

    def debugStr(self):
        
        childrenStr = str([n.name for n in self.children])
        parentStr = str([n.name for n in self.parents]) 
        return self.name + " children: " + childrenStr + " parents: " + parentStr 

    def __str__(self):
        
        return self.name


class OpNode(Node):

    def __init__(self, name, inRels, outRel):
        
        super(OpNode, self).__init__(name)
        self.inRels = inRels
        self.outRel = outRel
        # By default we assume that the operator requires data
        # to cross party boundaries. Override this for operators
        # where this is not the case
        self.isLocal = False
        self.isMPC = False
        # Indicates whether we want to split this operation
        # into local step and mpc step. By default we don't
        self.canSplit = False

    def requiresMPC(self):

        return self.outRel.isShared() and not self.isLocal

    def updateOpSpecificCols(self):
        # By default we don't need to do anything here
        return

    def replaceInRel(self, oldRel, newRel):

        # Not dealing with self-joins etc.
        assert(self.inRels.count(oldRel) == 1)
        self.inRels = [newRel if rel == oldRel else rel for rel in self.inRels]
        self.updateOpSpecificCols()

    def replaceParent(self, oldParent, newParent):

        self.parents.remove(oldParent)
        self.parents.add(newParent)
        self.replaceInRel(oldParent.outRel, newParent.outRel)

    def replaceChild(self, oldChild, newChild):

        self.children.remove(oldChild)
        self.children.add(newChild)


class Create(OpNode):

    def __init__(self, outRel):

        super(Create, self).__init__("create", [], outRel)
        # Input can be done by parties locally
        self.isLocal = True

    def __str__(self):

        colTypeStr = ", ".join([col.typeStr for col in self.outRel.columns])

        return "CREATE RELATION {} WITH COLUMNS ({})".format(
                self.outRel.name,
                colTypeStr 
            )


class Aggregate(OpNode):

    def __init__(self, inRel, outRel, keyCol, aggCol, aggregator):

        super(Aggregate, self).__init__("aggregation", [inRel], outRel)
        self.keyCol = keyCol
        self.aggCol = aggCol
        self.aggregator = aggregator
        # We are interested in splitting aggregations
        self.canSplit = True

    def updateOpSpecificCols(self):

        self.keyCol = self.inRels[0].columns[self.keyCol.idx]
        self.aggCol = self.inRels[0].columns[self.aggCol.idx]

    def __str__(self):

        return "AGG{} [{}, {}] FROM ({}) GROUP BY [{}] AS {}".format(
                "MPC" if self.isMPC else "",
                self.aggCol.getName(),
                self.aggregator,
                self.inRels[0].name,
                self.keyCol.getName(),
                self.outRel.name
            )

class Project(OpNode):

    def __init__(self, inRel, outRel, projector):

        super(Project, self).__init__("project", [inRel], outRel)
        # Projection can be done by parties locally
        self.isLocal = True

    def __str__(self):

        return "PROJECT{} [{}] FROM ({}) AS {}".format(
                "MPC" if self.isMPC else "",
                "dummy",
                self.inRels[0].name,
                self.outRel.name
            )


class Multiply(OpNode):

    def __init__(self, inRel, outRel, targetCol, operands):

        super(Multiply, self).__init__("multiply", [inRel], outRel)
        self.operands = operands
        self.targetCol = targetCol
        self.isLocal = True

    def __str__(self):

        operandStr = ", ".join([str(op) for op in self.operands])

        return "MUL [{}] FROM ({}) as {}".format(
                operandStr,
                self.inRels[0].name,
                self.outRel.name
            )


class Join(OpNode):

    def __init__(self, leftInRel, rightInRel, outRel, leftJoinCol, rightJoinCol):

        super(Join, self).__init__("join", [leftInRel, rightInRel], outRel)
        self.leftJoinCol = leftJoinCol
        self.rightJoinCol = rightJoinCol

    def __str__(self):

        return "({}) JOIN{} ({}) ON {} AND {} AS {}".format(
                self.inRels[0].name,
                "MPC" if self.isMPC else "",
                self.inRels[1].name,
                str(self.leftJoinCol),
                str(self.rightJoinCol),
                self.outRel.name
            )

    def updateOpSpecificCols(self):

        self.leftJoinCol = self.inRels[0].columns[self.leftJoinCol.idx]
        self.rightJoinCol = self.inRels[1].columns[self.rightJoinCol.idx]
 

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

    child.replaceParent(other, parent)
    parent.replaceChild(other, child)

    other.parents = set()
    other.children = set()
    other.inRels = []

def insertBetween(parent, child, other):

    assert(not other.children)
    assert(not other.parents)

    # Insert other below parent
    parent.addChild(other)
    other.inRels = [parent.outRel]
    other.updateOpSpecificCols()

    # Remove child from parent
    if child:
        child.replaceParent(parent, other)
        other.addChild(child)
    