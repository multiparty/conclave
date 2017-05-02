import copy

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

    def __str__(self):

        colStr = ", ".join([str(col) for col in self.columns])
        return "relation {}: ({})".format(self.name, colStr)

    # Returns the union of the collusion sets of all columns
    def getCombinedCollusionSet(self):

        colSets = [col.collusionSet for col in self.columns] 
        return reduce(lambda setA, setB: setA.union(setB), colSets)

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

    # side-effect on child
    def addChild(self, child):

        super(OpNode, self).addChild(child)
        child.inRels.append(self.outRel)

    def __str__(self):
        
        inRelStr = " ".join([str(rel) for rel in self.inRels])
        return "{} inputs: [{}] output: {}".format(self.name, inRelStr, str(self.outRel))

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

    def dfsPrint(self):
        
        self.dfsVisit(print)

class OpDag(Dag):

    def __init__(self, roots):
        
        super(OpDag, self).__init__(roots)

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
    
    # Create output relation--default column order is
    # key column first followed by column that will be aggregated
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

def _mergeCollusionSets(left, right):

    return left.union(right)

def join(leftInputNode, rightInputNode, leftColIdx, rightColIdx):

    # Get input relation from input nodes
    leftInRel = leftInputNode.outRel
    rightInRel = rightInputNode.outRel

    # Get columns from both relations
    leftCols = leftInRel.columns
    rightCols = rightInRel.columns

    # Get columns we will join on
    leftJoinCol = copy.copy(leftCols[leftColIdx])
    rightJoinCol = copy.copy(rightCols[rightColIdx])

    # Get the columns' merged collusion set
    mergedCollusionSet = _mergeCollusionSets(leftJoinCol)

    # Define output relation columns.
    # These will be the joined-on column followed
    # by all columns from left (other than join column)
    # and right (again excluding join column)

    # TODO: figure out what the collusion sets on the output 
    # should be for all but the join column
    # on one hand the original collusion set can reconstruct all
    # the values of the result column, however they don't necessarily
    # know the linkage

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
    colsInA = [("int", set([1])), ("int", set([2])), ("int", set([3]))]
    inA = create("inA", colsInA)

    # specify the workflow
    agg = aggregate(inA, "agg", 0, 1, None)
    projA = project(agg, "projA", None)
    projB = project(projA, "projB", None)

    # create dag and output
    dag = OpDag(set([inA]))
    dag.dfsPrint()
    
def opDagEx2():

    # define inputs
    colsInA = [("int", set([1])), ("int", set([2])), ("int", set([3]))]
    inA = create("inA", colsInA)

    # specify the workflow
    aggA = aggregate(inA, "aggA", 0, 1, None)
    projA = project(aggA, "projA", None)
    
    aggB = aggregate(inA, "aggB", 1, 2, None)
    projB = project(aggB, "projB", None)
    
    # create dag and output
    dag = OpDag(set([inA]))
    dag.dfsPrint()

if __name__ == "__main__":

    opDagEx2()
