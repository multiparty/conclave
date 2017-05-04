import copy
import functools

class Column():

    # For now we are assuming that a column has exactly one
    # collusion set, however this will change in the future
    def __init__(self, relName, typeStr, collusionSet):
        
        self.relName = relName
        self.typeStr = typeStr
        self.collusionSet = collusionSet

    def __str__(self):

        return "column {}".format(str(self.collusionSet))


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

        colSets = [col.collusionSet for col in self.columns] 
        return functools.reduce(lambda setA, setB: setA.union(setB), colSets)

    def __str__(self):

        colStr = ", ".join([str(col) for col in self.columns])
        return "relation {}: ({})".format(self.name, colStr)


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

    def requiresMPC(self):

        return self.outRel.isShared() and not self.isLocal

    # This method pushes the given node between it
    # and each of its children
    def insertAfterSelf(self, node):

        for child in self.children:
            child.parents.remove(self)
            node.addChild(child)

        self.children = set()
        self.addChild(node)

    def __str__(self):
        
        inRelStr = " ".join([str(rel) for rel in self.inRels])
        suffix = "mpc" if self.isMPC else ""
        return "{} inputs: [{}] output: {}".format(self.name + suffix, inRelStr, str(self.outRel))


class Create(OpNode):

    def __init__(self, outRel):

        super(Create, self).__init__("create", [], outRel)
        # Input can be done by parties locally
        self.isLocal = True


class Aggregate(OpNode):

    def __init__(self, inRel, outRel, keyCol, aggCol, aggregator):

        super(Aggregate, self).__init__("aggregation", [inRel], outRel)
        self.keyCol = keyCol
        self.aggCol = aggCol
        self.aggregator = aggregator


class Project(OpNode):

    def __init__(self, inRel, outRel, projector):

        super(Project, self).__init__("project", [inRel], outRel)
        # Projection can be done by parties locally
        self.isLocal = True    


class Join(OpNode):

    def __init__(self, leftInRel, rightInRel, outRel, leftJoinCol, rightJoinCol):

        super(Join, self).__init__("join", [leftInRel, rightInRel], outRel)
        self.leftJoinCol = leftJoinCol
        self.rightJoinCol = rightJoinCol


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
    # Side-effects on all input other than node
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


def _mergeCollusionSets(left, right):

    return left.union(right)

def create(name, columns):

    columns = [Column(name, typeStr, collusionSet) for typeStr, collusionSet in columns]
    outRel = Relation(name, columns)
    op = Create(outRel)
    return op

def aggregate(inputOpNode, outputName, keyColIdx, aggColIdx, aggregator):

    # Get input relation from input node
    inRel = inputOpNode.outRel

    # Get relevant columns and create copies
    inCols = inRel.columns
    keyCol = copy.copy(inCols[keyColIdx])
    aggCol = copy.copy(inCols[aggColIdx])
    
    # Create output relation. Default column order is
    # key column first followed by column that will be 
    # aggregated
    outRelCols = [keyCol, aggCol]
    outRel = Relation(outputName, outRelCols)

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

    # Create our operator node
    op = Project(inRel, outRel, projector)
    
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

def pushDownMPC(sortedNodes):
    
    for node in sortedNodes:
        # print("node:", node)
        parents = node.parents

        if len(parents) == 0:
            # we are at a root node
            if node.requiresMPC():
                if isinstance(node, Aggregate):
                    mpcAggNode = copy.deepcopy(node)
                    mpcAggNode.outRel.rename(node.outRel.name + "_obl")
                    mpcAggNode.isMPC = True
                    mpcAggNode.children = set()
                    mpcAggNode.parents = set()
                    node.insertAfterSelf(mpcAggNode)
                else:
                    node.isMPC = True
        elif len(parents) == 1:
            # see if we can pull down any MPC ops
            parent = next(iter(parents))
            # print("parent:", parent)

            if parent.isMPC:
                if isinstance(parent, Aggregate):
                    node.isMPC = True
                else:
                    node.isMPC = True
            else:
                if node.requiresMPC():
                    if isinstance(node, Aggregate):
                        mpcAggNode = copy.deepcopy(node)
                        mpcAggNode.outRel.rename(node.outRel.name + "_obl")
                        mpcAggNode.isMPC = True
                        mpcAggNode.children = set()
                        mpcAggNode.parents = set()
                        node.insertAfterSelf(mpcAggNode)
                    else:
                        node.isMPC = True
        else:
            node.isMPC = True

def rewriteDag(dag):

    sortedNodes = dag.topSort()
    pushDownMPC(sortedNodes)
    result = dag.topSort()
    print("\n".join([str(node) for node in result]))

def simpleDagExample():

    a = Node("a")
    b = Node("b")
    c = Node("c")
    d = Node("d")
    e = Node("e")

    a.addChild(b)
    a.addChild(c)

    b.addChild(c)
    b.addChild(d)

    c.addChild(d)

    e.addChild(d)

    dag = Dag(set([a, e]))
    dag.dfsPrint()

def opDagEx1():

    # define inputs
    colsInA = [("int", set([1, 2, 3])), ("int", set([1, 2, 3])), ("int", set([1, 2, 3]))]
    inA = create("inA", colsInA)

    # specify the workflow
    agg = aggregate(inA, "agg", 0, 1, None)
    projA = project(agg, "projA", None)
    projB = project(projA, "projB", None)

    # create dag with root nodes
    dag = OpDag(set([inA]))
    
    # compile to MPC
    rewriteDag(dag)

def opDagEx2():

    # define inputs
    colsInA = [("int", set([1, 2])), ("int", set([1, 2])), ("int", set([1, 2]))]
    inA = create("inA", colsInA)

    # specify the workflow
    aggA = aggregate(inA, "aggA", 0, 1, None)
    projA = project(aggA, "projA", None)
    
    aggB = aggregate(inA, "aggB", 1, 2, None)
    projB = project(aggB, "projB", None)
    
    # create dag with roots nodes
    dag = OpDag(set([inA]))
    
    # compile to MPC
    rewriteDag(dag)

def opDagEx3():

    # define inputs
    colsInA = [("int", set([1])), ("int", set([1]))]
    inA = create("inA", colsInA)

    colsInB = [("int", set([2])), ("int", set([2]))]
    inB = create("inB", colsInB)

    # specify the workflow
    aggA = aggregate(inA, "aggA", 0, 1, None)
    projA = project(aggA, "projA", None)
    
    aggB = aggregate(inB, "aggB", 0, 1, None)
    projB = project(aggB, "projB", None)
    
    joined = join(projA, projB, "joined", 0, 0)

    projected = project(joined, "projected", None)
    aggregated = aggregate(projected, "aggregated", 0, 1, None)

    # create dag
    dag = OpDag(set([inA, inB]))
    
    # compile to MPC
    rewriteDag(dag)

if __name__ == "__main__":

    opDagEx2()
