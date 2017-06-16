import copy
import salmon.utils as utils
import salmon.dag as saldag
import warnings

def pushOpNodeDown(topNode, bottomNode):

    # only dealing with one grandchild case for now
    assert(len(bottomNode.children) <= 1)
    child = next(iter(bottomNode.children), None)

    # remove bottom node between the bottom node's child
    # and the top node
    saldag.removeBetween(topNode, child, bottomNode)

    # we need all parents of the parent node
    grandParents = copy.copy(topNode.getSortedParents())

    # we will insert the removed bottom node between
    # each parent of the top node and the top node
    for idx, grandParent in enumerate(grandParents):
        toInsert = copy.deepcopy(bottomNode)
        toInsert.outRel.rename(toInsert.outRel.name + "_" + str(idx))
        toInsert.parents = set()
        toInsert.children = set()
        saldag.insertBetween(grandParent, topNode, toInsert)
        toInsert.updateStoredWith() 

def splitNode(node):

    # Only dealing with single child case for now
    assert(len(node.children) <= 1)
    clone = copy.deepcopy(node)
    clone.outRel.rename(node.outRel.name + "_obl")
    clone.parents = set()
    clone.children = set()
    clone.isMPC = True
    child = next(iter(node.children), None)
    saldag.insertBetween(node, child, clone)

def forkNode(node):

    # we can skip the first child
    childIt = enumerate(copy.copy(node.getSortedChildren()))
    next(childIt)
    # clone node for each of the remaining children
    for idx, child in childIt:
        # create clone and rename output relation to
        # avoid identical relation names for different nodes
        clone = copy.deepcopy(node)
        clone.outRel.rename(node.outRel.name + "_" + str(idx))
        clone.parents = copy.copy(node.parents)
        warnings.warn("hacky forkNode")
        clone.ordered = copy.copy(node.ordered)
        clone.children = set([child])
        for parent in clone.parents:
            parent.children.add(clone)
        node.children.remove(child)
        # make cloned node the child's new parent
        child.replaceParent(node, clone)
        child.updateOpSpecificCols()

class DagRewriter:

    def __init__(self):

        # If true we visit topological ordering of dag in reverse
        self.reverse = False

    def rewrite(self, dag):

        ordered = dag.topSort()
        if self.reverse:
            ordered = ordered[::-1]

        for node in ordered:
            print(type(self).__name__, "rewriting", node.outRel.name)
            if isinstance(node, saldag.Aggregate):
                self._rewriteAggregate(node)
            elif isinstance(node, saldag.Project):
                self._rewriteProject(node)
            elif isinstance(node, saldag.Multiply):
                self._rewriteMultiply(node)
            elif isinstance(node, saldag.Join):
                self._rewriteJoin(node)
            elif isinstance(node, saldag.Concat):
                self._rewriteConcat(node)
            elif isinstance(node, saldag.Store):
                self._rewriteStore(node)
            elif isinstance(node, saldag.Create):
                self._rewriteCreate(node)
            else:
                msg = "Unknown class " + type(node).__name__
                raise Exception(msg)

class MPCPushDown(DagRewriter):

    def __init__(self):

        super(MPCPushDown, self).__init__()

    def _rewriteDefault(self, node):

        node.isMPC = node.requiresMPC()

    def _rewriteUnaryDefault(self, node):

        parent = next(iter(node.parents))
        if parent.isMPC:
            # if we have an MPC parent we can try and pull it down
            # the leaf condition is to avoid issues with storedWith
            # getting overwritten
            # TODO: think of a better way to handle the leaf node case
            if isinstance(parent, saldag.Concat) and parent.isBoundary() and not node.isLeaf():
                pushOpNodeDown(parent, node)
            else:
                node.isMPC = True
        else:
            pass

    def _rewriteAggregate(self, node):

        parent = next(iter(node.parents))
        if parent.isMPC:
            if isinstance(parent, saldag.Concat) and parent.isBoundary():
                splitNode(node)
                pushOpNodeDown(parent, node)
            else:
                node.isMPC = True
        else:
            pass

    def _rewriteProject(self, node):

        self._rewriteUnaryDefault(node)

    def _rewriteMultiply(self, node):

        self._rewriteUnaryDefault(node)

    def _rewriteJoin(self, node):

        self._rewriteDefault(node)

    def _rewriteConcat(self, node):

        if node.requiresMPC():
            node.isMPC = True
            if len(node.children) > 1 and node.isBoundary():
                forkNode(node)

    def _rewriteStore(self, node):

        pass

    def _rewriteCreate(self, node):

        pass

class MPCPushUp(DagRewriter):

    def __init__(self):

        super(MPCPushUp, self).__init__()
        self.reverse = True

    def _rewriteUnaryDefault(self, node):

        par = next(iter(node.parents))
        if node.isReversible() and node.isLowerBoundary() and not par.isRoot():
            node.getInRel().storedWith = copy.copy(node.outRel.storedWith)
            node.isMPC = False

    def _rewriteAggregate(self, node):

        pass

    def _rewriteProject(self, node):

        self._rewriteUnaryDefault(node)

    def _rewriteMultiply(self, node):

        self._rewriteUnaryDefault(node)

    def _rewriteJoin(self, node):

        if node.isLowerBoundary():    
            leftStoredWith = node.getLeftInRel().storedWith
            rightStoredWith = node.getRightInRel().storedWith
            outStoredWith = node.outRel.storedWith

            revealJoinOp = None
            if outStoredWith == leftStoredWith:
                # sanity check
                assert outStoredWith != rightStoredWith
                revealJoinOp = saldag.RevealJoin.fromJoin(
                    node, node.getLeftInRel(), outStoredWith)
            elif outStoredWith == rightStoredWith:
                revealJoinOp = saldag.RevealJoin.fromJoin(
                    node, node.getLeftInRel(), outStoredWith)
                
            if revealJoinOp:
                parents = revealJoinOp.parents
                for par in parents:
                    par.replaceChild(node, revealJoinOp)

    def _rewriteConcat(self, node):

        # concats are always reversible so we just need to know
        # if we're dealing with a boundary node
        if node.isLowerBoundary():

            outStoredWith = node.outRel.storedWith
            for par in node.parents:
                if not par.isRoot():
                    par.outRel.storedWith = copy.copy(outStoredWith)
            node.isMPC = False

    def _rewriteStore(self, node):

        pass

    def _rewriteCreate(self, node):

        pass

class CollSetPropDown(DagRewriter):

    def __init__(self):

        super(CollSetPropDown, self).__init__()

    def _rewriteUnaryDefault(self, node):
        
        pass

    def _rewriteAggregate(self, node):
        
        inCols = node.getInRel().columns

        # TODO: this seems awkward. re-think keyCol, aggCol etc.
        inKeyCol = utils.find(inCols, node.keyCol.getName())
        outKeyCol = node.outRel.columns[0]
        outKeyCol.collSets |= copy.deepcopy(inKeyCol.collSets)

        inAggCol = utils.find(inCols, node.aggCol.getName())
        outAggCol = node.outRel.columns[1]
        outAggCol.collSets |= copy.deepcopy(inAggCol.collSets)

    def _rewriteProject(self, node):

        pass

    def _rewriteMultiply(self, node):

        pass

    def _rewriteJoin(self, node):

        # TODO: technically this should take in a start index as well
        # This helper method takes in a relation, the key column of the join 
        # and its index. 
        # It returns a list of new columns with correctly merged collusion sets
        # for the output relation (in the same order as they appear on the input
        # relation but excluding the key column)
        def _colsFromRel(relation, keyCol, keyColIdx):

            resultCols = []
            for idx, col in enumerate(relation.columns):
                # Exclude key column
                if idx != keyColIdx:
                    # This is somewhat nuanced. The collusion set
                    # of col knows the values of the result but not
                    # the linkage of these values to the key column values.
                    # Thus we must take the union of the collusion set of
                    # col *and* the collusion set of the key column for the
                    # new column.
                    newColSet = utils.mergeCollusionSets(
                        col.collusionSet, keyCol.collusionSet)

                    newCol = rel.Column(
                        outputName, idx, col.typeStr, newColSet)
                    
                    resultCols.append(newCol)

            return resultCols

        leftInRel = node.getLeftInRel()
        rightInRel = node.getRightInRel()

        leftJoinCol = utils.find(leftInRel.columns, node.leftJoinCol.getName())
        rightJoinCol = utils.find(rightInRel.columns, node.rightJoinCol.getName())

        outJoinCol = node.outRel.columns[0]
        keyColCollSets = utils.mergeCollSets(leftJoinCol.collSets, rightJoinCol.collSets)
        outJoinCol.collSets = keyColCollSets

        absIdx = 1
        for inCol in leftInRel.columns:
            if inCol != leftJoinCol:
                node.outRel.columns[absIdx].collSets = utils.mergeCollSets(
                    keyColCollSets, inCol.collSets) 
                absIdx += 1

        for inCol in rightInRel.columns:
            if inCol != rightJoinCol:
                node.outRel.columns[absIdx].collSets = utils.mergeCollSets(
                    keyColCollSets, inCol.collSets) 
                absIdx += 1

    def _rewriteConcat(self, node):

        # Copy over columns from existing relation 
        outRelCols = node.outRel.columns
        
        # Combine per-column collusion sets
        for idx, col in enumerate(outRelCols):
            columnsAtIdx = [inRel.columns[idx] for inRel in node.getInRels()]
            col.collSets = utils.collSetsFromColumns(columnsAtIdx)

    def _rewriteStore(self, node):
        
        pass

    def _rewriteCreate(self, node):
        
        pass

def rewriteDag(dag):

    MPCPushDown().rewrite(dag)
    # ironic?
    MPCPushUp().rewrite(dag)
    CollSetPropDown().rewrite(dag)
    return dag

def scotch(f):

    from salmon.codegen import scotch

    def wrap():
        code = scotch.ScotchCodeGen(f())._generate(None, None)
        return code

    return wrap

def mpc(f):

    def wrap():
        dag = rewriteDag(saldag.OpDag(f()))
        return dag

    return wrap