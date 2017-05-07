import copy
import functools
import utils

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

    def __str__(self):

        return self.getName()


class Relation():

    def __init__(self, name, columns):
        
        self.name = name
        self.columns = columns


    def rename(self, newName):

        self.name = newName
        for col in self.columns:
            col.relName = newName

    def isShared(self):

        return len(self.getCombinedCollusionSet()) > 1

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

    def __str__(self):
        
        inRelStr = " ".join([str(rel) for rel in self.inRels])
        suffix = "mpc" if self.isMPC else ""
        return "{} inputs: [{}] output: {}".format(self.name + suffix, inRelStr, str(self.outRel))


class Create(OpNode):

    def __init__(self, outRel):

        super(Create, self).__init__("create", [], outRel)
        # Input can be done by parties locally
        self.isLocal = True

    def __str__(self):

        colTypeStr = ", ".join([col.typeStr for col in self.outRel.columns])

        return "CREATE RELATION {} WITH COLUMNS ({}),".format(
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

        return "AGG{} [{}, {}] FROM ({}) GROUP BY [{}] AS {},".format(
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

        return "{} = project{}({})".format(
                self.outRel.name,
                "MPC" if self.isMPC else "",
                self.inRels[0].name,
            )

class Multiply(OpNode):

    def __init__(self, inRel, outRel, targetCol, operands):

        super(Multiply, self).__init__("multiply", [inRel], outRel)
        self.operands = operands
        self.targetCol = targetCol
        self.isLocal = True

    def __str__(self):

        operandStr = "*".join([str(op) for op in self.operands])

        return "{} = multiply{}({}, {} -> {})".format(
                self.outRel.name,
                "MPC" if self.isMPC else "",
                self.inRels[0].name,
                operandStr,
                str(self.targetCol)   
            )


class Join(OpNode):

    def __init__(self, leftInRel, rightInRel, outRel, leftJoinCol, rightJoinCol):

        super(Join, self).__init__("join", [leftInRel, rightInRel], outRel)
        self.leftJoinCol = leftJoinCol
        self.rightJoinCol = rightJoinCol

    def __str__(self):

        return "{} = join{}({}, {})".format(
                self.outRel.name,
                "MPC" if self.isMPC else "",
                self.inRels[0].name,
                self.inRels[1].name
            )


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
        return "\n".join(str(node) for node in order)


def _mergeCollusionSets(left, right):

    return left.union(right)

def create(name, columns):

    columns = [Column(name, idx, typeStr, collusionSet) 
        for idx, (typeStr, collusionSet) in enumerate(columns)]
    outRel = Relation(name, columns)
    op = Create(outRel)
    return op

def aggregate(inputOpNode, outputName, keyColName, aggColName, aggregator):

    # Get input relation from input node
    inRel = inputOpNode.outRel

    # Get relevant columns
    inCols = inRel.columns
    keyCol = utils.find(inCols, keyColName)
    aggCol = utils.find(inCols, aggColName)
    
    # Create output relation. Default column order is
    # key column first followed by column that will be 
    # aggregated. Note that we want copies as these are
    # copies on the output relation and changes to them
    # shouldn't affect the original columns
    outRelCols = [copy.deepcopy(keyCol), copy.deepcopy(aggCol)]
    outRel = Relation(outputName, outRelCols)
    outRel.updateColumns()

    # Create our operator node
    op = Aggregate(inRel, outRel, keyCol, aggCol, aggregator)
    
    # Add it as a child to input node 
    inputOpNode.addChild(op)

    return op

def project(inputOpNode, outputName, projector):

    # Get input relation from input node
    inRel = inputOpNode.outRel

    # Get relevant columns and create copies
    outRelCols = copy.deepcopy(inRel.columns)
    
    # Create output relation
    outRel = Relation(outputName, outRelCols)
    outRel.updateColumns()

    # Create our operator node
    op = Project(inRel, outRel, projector)
    
    # Add it as a child to input node 
    inputOpNode.addChild(op)

    return op

def multiply(inputOpNode, outputName, targetColName, operands):

    # Get input relation from input node
    inRel = inputOpNode.outRel

    # Get relevant columns and create copies
    outRelCols = copy.deepcopy(inRel.columns)
    
    # Create result column. By default we add it to the
    # output relation as the first column
    # Its collusion set is the union of all operand collusion
    # sets

    # Replace all column names with corresponding columns.
    # Constants will be replaced with empty sets 
    # (indicating an empty collusion set for the next step)
    operands = [utils.find(outRelCols, op) if isinstance(op, str) else op for op in operands]
    operands = copy.deepcopy(operands)

    # Update target column collusion set
    targetCollusionSet = utils.collusionSetUnion(operands)
    targetColumn = utils.find(outRelCols, targetColName)
    targetColumn.collusionSet = targetCollusionSet
    
    # Create output relation
    outRel = Relation(outputName, outRelCols)
    outRel.updateColumns()

    # Create our operator node
    op = Multiply(inRel, outRel, targetColumn, operands)
    
    # Add it as a child to input node 
    inputOpNode.addChild(op)

    return op

# TODO: is a self-join a problem?
def join(leftInputNode, rightInputNode, outputName, leftColIdx, rightColIdx):

    # This helper method takes in a relation, the key column of the join 
    # and its index. 
    # It returns a list of new columns with correctly merged collusion sets
    # for the output relation (in the same order as they appear on the input
    # relation but excluding the key column)
    def _colsFromRel(rel, keyCol, keyColIdx):

        resultCols = []
        for idx, col in enumerate(rel.columns):
            # Exclude key column
            if idx != keyColIdx:
                # This is somewhat nuanced. The collusion set
                # of col knows the values of the result but not
                # the linkage of these values to the key column values.
                # Thus we must take the union of the collusion set of
                # col *and* the collusion set of the key column for the
                # new column.
                newColSet = _mergeCollusionSets(
                    col.collusionSet, keyCol.collusionSet)

                newCol = Column(
                    outputName, col.typeStr, newColSet)
                
                resultCols.append(newCol)

        return resultCols

    # Get input relation from input nodes
    leftInRel = leftInputNode.outRel
    rightInRel = rightInputNode.outRel

    # Get columns from both relations
    leftCols = leftInRel.columns
    rightCols = rightInRel.columns

    # Get columns we will join on
    leftJoinCol = leftCols[leftColIdx]
    rightJoinCol = rightCols[rightColIdx]

    # Get the key columns' merged collusion set
    keyCollusionSet = _mergeCollusionSets(
        leftJoinCol.collusionSet, rightJoinCol.collusionSet)

    # Create new key column
    outKeyCol = Column(
        outputName, leftJoinCol.typeStr, keyCollusionSet)


    # Define output relation columns.
    # These will be the key column followed
    # by all columns from left (other than join column)
    # and right (again excluding join column)
    outRelCols = [outKeyCol] \
               + _colsFromRel(leftInRel, outKeyCol, leftColIdx) \
               + _colsFromRel(rightInRel, outKeyCol, rightColIdx)

    # Create output relation
    outRel = Relation(outputName, outRelCols)
    outRel.updateColumns()

    # Create join operator
    op = Join(
        leftInRel, 
        rightInRel, 
        outRel, 
        leftJoinCol, 
        rightJoinCol
    )

    # Add it as a child to both input nodes 
    leftInputNode.addChild(op)
    rightInputNode.addChild(op)

    return op

def removeBetween(parent, child, other):

    assert(len(other.children) < 2)
    assert(len(other.parents) < 2)

    parent.children.remove(other)
    other.inRels = []
    other.parents = set()
    other.children = set()

    child.parents.remove(other)
    child.inRels.remove(other.outRel)

    parent.addChild(child)
    child.inRels.append(parent.outRel)

def insertBetween(parent, child, other):

    # Insert other below parent
    parent.addChild(other)
    other.inRels = [parent.outRel]

    print(other.inRels[0])
    # We need to update agg and key cols
    # on Aggregation nodes etc.
    other.updateOpSpecificCols()

    # If parent is a leaf, we are done
    # otherwise we need to update the child
    # and add it as a child to other

    if child:
        # Disconnect parent and child
        parent.children.remove(child)
        child.parents.remove(parent)
        child.inRels.remove(parent.outRel)

        # Insert child below other
        other.addChild(child)
        child.inRels.append(other.outRel)


def opNodesCommute(nodeA, nodeB):
    
    # This is incomplete. We are only interested in Aggregations
    # in relation to other operations for now

    if isinstance(nodeA, Aggregate):
        if isinstance(nodeB, Project):
            return True

    return False

def getNewMpcNode(node, suffix):

    newNode = copy.deepcopy(node)
    newNode.outRel.rename(node.outRel.name + "_obl_" + suffix)
    newNode.isMPC = True
    newNode.children = set()
    newNode.parents = set()
    return newNode

def pushOpNodeDown(parent, node):

    # the only nodes we push down are aggregation nodes
    # we have created and so we know that these are single
    # parent nodes
    assert(len(parent.parents) == 1)
    grandParent = next(iter(parent.parents))
    
    # remove MPC aggregation between current node
    # and grand parent
    removeBetween(grandParent, node, parent)

    # Need copy of node.children because we are 
    # updating node.children inside the loop
    tempChildren = copy.copy(node.children)

    if not tempChildren:
        insertBetween(node, None, parent)
    
    for idx, child in enumerate(tempChildren):
        insertBetween(node, child, getNewMpcNode(parent, str(idx)))

def splitNode(node):

    # Need copy of node.children because we are 
    # updating node.children inside the loop
    tempChildren = copy.copy(node.children)
    
    if not tempChildren:
        insertBetween(node, None, getNewMpcNode(node, "0"))

    # We insert an mpc-agg node per child
    for idx, child in enumerate(tempChildren):
        insertBetween(node, child, getNewMpcNode(node, str(idx)))

def pushDownMPC(sortedNodes):
    
    for node in sortedNodes:
        parents = node.parents

        if len(parents) == 1:
            # see if we can pull down any MPC ops
            parent = next(iter(parents))

            if parent.isMPC:
                # The parent node is in MPC mode which means
                # that it is either an MPC aggregation we can try
                # and push down or another op in which case we must
                # switch to mpc mode for the current node

                # TODO: this is not entirely correct!
                # we need to check if all nodes *above* the parent
                # are local 
                if opNodesCommute(parent, node):
                    pushOpNodeDown(parent, node)
                else:
                    node.isMPC = True
            else:
                # We are still in local mode. If the current node
                # does not require MPC, there's nothing to do, otherwise
                # we need to check if we can split the operation 
                if node.requiresMPC():
                    
                    if node.canSplit:
                        splitNode(node)
                    else:
                        node.isMPC = True

        elif len(parents) == 2:
            node.isMPC = True

def rewriteDag(dag):

    sortedNodes = dag.topSort()
    pushDownMPC(sortedNodes)
    print(dag)
