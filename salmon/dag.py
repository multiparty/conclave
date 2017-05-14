from salmon import rel

# TODO: introduce abstract methods

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

    def requiresMPC(self):

        return self.outRel.isShared() and not self.isLocal

    def updateOpSpecificCols(self):
        # By default we don't need to do anything here
        return

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

    def makeOrphan(self):

        self.parents = set()
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

    def makeOrphan(self):

        self.parents = set()
        self.leftParent = None
        self.rightParent = None

    def replaceParent(self, oldParent, newParent):
        super(BinaryOpNode, self).replaceParent(oldParent, newParent)
        if self.leftParent == oldParent:
            self.leftParent = newParent
        elif self.rightParent == oldParent:
            self.rightParent = newParent
    

class Create(UnaryOpNode):

    def __init__(self, outRel):

        super(Create, self).__init__("create", outRel, None)
        # Input can be done by parties locally
        self.isLocal = True

    def __str__(self):

        colTypeStr = ", ".join([col.typeStr for col in self.outRel.columns])

        return "CREATE RELATION {} WITH COLUMNS ({})".format(
                self.outRel.name,
                colTypeStr 
            )


class Aggregate(UnaryOpNode):

    def __init__(self, outRel, parent, keyCol, aggCol, aggregator):

        super(Aggregate, self).__init__("aggregation", outRel, parent)
        self.keyCol = keyCol
        self.aggCol = aggCol
        self.aggregator = aggregator
        # We are interested in splitting aggregations
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

    def __init__(self, outRel, parent, projector):

        super(Project, self).__init__("project", outRel, parent)
        # Projections can be done by parties locally
        self.isLocal = True

    def __str__(self):

        return "PROJECT{} [{}] FROM ({}) AS {}".format(
                "MPC" if self.isMPC else "",
                "dummy",
                self.getInRel().name,
                self.outRel.name
            )


class Multiply(UnaryOpNode):

    def __init__(self, outRel, parent, targetCol, operands):

        super(Multiply, self).__init__("multiply", outRel, parent)
        self.operands = operands
        self.targetCol = targetCol
        self.isLocal = True

    def __str__(self):

        operandStr = ", ".join([str(op) for op in self.operands])

        return "MUL [{}] FROM ({}) as {}".format(
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
    parent.replaceChild(other, child)

    other.makeOrphan()
    other.children = set()

def insertBetween(parent, child, other):

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
        child.updateOpSpecificCols()
        other.children.add(child)
    