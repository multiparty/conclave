import copy
import salmon.utils as utils
import salmon.dag as dag

def pushOpNodeDown(topNode, bottomNode):

    # only dealing with one grandchild case for now
    assert(len(bottomNode.children) == 1)
    child = next(iter(bottomNode.children))

    # remove bottom node between the bottom node's child
    # and the top node
    dag.removeBetween(topNode, child, bottomNode)

    # we need all parents of the parent node
    grandParents = copy.copy(topNode.parents)

    # we will insert the removed bottom node between
    # each parent of the top node and the top node
    for idx, grandParent in enumerate(grandParents):
        toInsert = copy.deepcopy(bottomNode)
        toInsert.outRel.rename(toInsert.outRel.name + "_" + str(idx))
        toInsert.parents = set()
        toInsert.children = set()
        dag.insertBetween(grandParent, topNode, toInsert)

def splitNode(node):

    # Only dealing with single child case for now
    assert(len(node.children) == 1)
    clone = copy.deepcopy(node)
    clone.outRel.rename(node.outRel.name + "_obl")
    clone.parents = set()
    clone.children = set()
    clone.isMPC = True
    child = next(iter(node.children))
    dag.insertBetween(node, child, clone)

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

def pushDownMPC(sortedNodes, graph=None):

    def visitDefault(node):

        node.isMPC = node.requiresMPC()

    def visitUnaryDefault(node):

        parent = next(iter(node.parents))
        if parent.isMPC:
            # if we have an MPC parent we can try and pull it down
            if isinstance(parent, dag.Concat) and parent.isBoundary():
                pushOpNodeDown(parent, node)
            else:
                node.isMPC = True
        else:
            pass

    def visitConcat(node):

        if node.requiresMPC():
            node.isMPC = True
            if len(node.children) > 1 and node.isBoundary():
                forkNode(node)

    def visitAggregate(node):

        parent = next(iter(node.parents))
        if parent.isMPC:
            # if we have an MPC parent we can try and pull it down
            if isinstance(parent, dag.Concat) and parent.isBoundary():
                splitNode(node)
                pushOpNodeDown(parent, node)
            else:
                node.isMPC = True
        else:
            pass

    for node in sortedNodes:

        print("Visiting", node.outRel.name)

        if isinstance(node, dag.Aggregate):

            visitAggregate(node)

        elif isinstance(node, dag.Project):

            visitUnaryDefault(node)

        elif isinstance(node, dag.Multiply):

            visitUnaryDefault(node)

        elif isinstance(node, dag.Join):

            node.isMPC = node.requiresMPC()

        elif isinstance(node, dag.Concat):

            visitConcat(node)

        elif isinstance(node, dag.Store):

            continue

        elif isinstance(node, dag.Create):

            continue

        else:

            assert(False)

        if graph:
            print("################")
            print(str(graph))
            print("################")

def rewriteDag(dag):

    sortedNodes = dag.topSort()
    pushDownMPC(sortedNodes)
    # ironic?
    # pushUpMPC(sortedNodes[::-1])
    return dag

def mpc(f):

    def wrap():
        beerProg = rewriteDag(dag.OpDag(f()))
        return beerProg

    return wrap
