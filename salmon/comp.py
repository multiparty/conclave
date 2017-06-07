import copy
import salmon.utils as utils
import salmon.dag as saldag

def pushOpNodeDown(topNode, bottomNode):

    # only dealing with one grandchild case for now
    assert(len(bottomNode.children) == 1)
    child = next(iter(bottomNode.children))

    # remove bottom node between the bottom node's child
    # and the top node
    saldag.removeBetween(topNode, child, bottomNode)

    # we need all parents of the parent node
    grandParents = copy.copy(topNode.parents)

    # we will insert the removed bottom node between
    # each parent of the top node and the top node
    for idx, grandParent in enumerate(grandParents):
        toInsert = copy.deepcopy(bottomNode)
        toInsert.outRel.rename(toInsert.outRel.name + "_" + str(idx))
        toInsert.parents = set()
        toInsert.children = set()
        saldag.insertBetween(grandParent, topNode, toInsert)

def splitNode(node):

    # Only dealing with single child case for now
    assert(len(node.children) == 1)
    clone = copy.deepcopy(node)
    clone.outRel.rename(node.outRel.name + "_obl")
    clone.parents = set()
    clone.children = set()
    clone.isMPC = True
    child = next(iter(node.children))
    saldag.insertBetween(node, child, clone)

def forkNode(node):

    # we can skip the first child
    childIt = enumerate(copy.copy(node.children))
    next(childIt)
    # clone node for each of the remaining children
    for idx, child in childIt:
        # create clone and rename output relation to
        # avoid identical relation names for different nodes
        clone = copy.deepcopy(node)
        clone.outRel.rename(node.outRel.name + "_" + str(idx))
        clone.parents = copy.copy(node.parents)
        clone.children = set([child])
        for parent in clone.parents:
            parent.children.add(clone)
        node.children.remove(child)
        # make cloned node the child's new parent
        child.replaceParent(node, clone)
        child.updateOpSpecificCols()

def propagateCollSets(sortedNodes):

    pass

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
            if isinstance(parent, saldag.Concat) and parent.isBoundary():
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

        if node.isReversible() and node.isLowerBoundary():
            print("Reversing", node.outRel.name)
            node.getInRel().storedWith = copy.copy(node.outRel.storedWith)
            node.isMPC = False

    def _rewriteAggregate(self, node):

        pass

    def _rewriteProject(self, node):

        self._rewriteUnaryDefault(node)

    def _rewriteMultiply(self, node):

        self._rewriteUnaryDefault(node)

    def _rewriteJoin(self, node):

        pass

    def _rewriteConcat(self, node):

    	# concats are always reversible so we just need to know
    	# if we're dealing with a boundary node
        if node.isLowerBoundary():

        	outStoredWith = node.outRel.storedWith
        	inRels = node.getInRels()
        	for inRel in inRels:
        		inRel.storedWith = copy.copy(outStoredWith)
        	node.isMPC = False

    def _rewriteStore(self, node):

        assert(not node.isMPC)
        node.getInRel().storedWith = copy.copy(node.outRel.storedWith)

    def _rewriteCreate(self, node):

        pass

def rewriteDag(dag):

    MPCPushDown().rewrite(dag)
    # ironic?
    # MPCPushUp().rewrite(dag)
    return dag

def scotch(f):

	from salmon.codegen import scotch

	def wrap():
		scotch.ScotchCodeGen(f()).generate(None, None)

	return wrap

def mpc(f):

    def wrap():
        dag = rewriteDag(saldag.OpDag(f()))
        return dag

    return wrap
