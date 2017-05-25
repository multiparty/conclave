import copy
import salmon.utils as utils
import salmon.dag as dag

def opNodesCommute(nodeA, nodeB):
    
    # This is incomplete. We are only interested in Aggregations
    # in relation to other operations for now

    if isinstance(nodeA, dag.Aggregate):
        if isinstance(nodeB, dag.Project):
            return True

    return False

# TODO: This is hacky
def getNewMpcNode(node, suffix):

    assert(isinstance(node, dag.Aggregate))
    newNode = copy.deepcopy(node)
    newNode.outRel.rename(node.outRel.name + "_obl_" + suffix)
    newNode.keyCol.idx = 0
    newNode.aggCol.idx = 1
    newNode.isMPC = True
    newNode.children = set()
    newNode.makeOrphan()
    return newNode

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
        dag.insertBetween(grandParent, topNode, toInsert)

def splitNode(node):

    # Need copy of node.children because we are 
    # updating node.children inside the loop
    tempChildren = copy.copy(node.children)
    
    if not tempChildren:
        dag.insertBetween(node, None, getNewMpcNode(node, "0"))

    # We insert an mpc-agg node per child
    for idx, child in enumerate(tempChildren):
        dag.insertBetween(node, child, getNewMpcNode(node, str(idx)))
        
def forkNode(node):

    for child in node.children:
        # TODO
        pass

def pushDownMPC(sortedNodes):

    def visitUnaryDefault(node):

        parent = next(iter(node.parents))
        if parent.isMPC:
            # check if we have an MPC parent we can try
            # and pull down
            if isinstance(parent, dag.Concat) and parent.isBoundary():
                pushOpNodeDown(parent, node)
            else:
                node.isMPC = True
        else:
            pass

    def visitConcat(node):

        if node.requiresMPC():
            if len(node.children) > 1:
                forkNode(node)
            node.isMPC = True
        
    for node in sortedNodes:

        if isinstance(node, dag.Aggregate):

            node.isMPC = node.requiresMPC()

        elif isinstance(node, dag.Project):

            visitUnaryDefault(node)

        elif isinstance(node, dag.Multiply):

            visitUnaryDefault(node)
        
        elif isinstance(node, dag.Join):

            node.isMPC = node.requiresMPC()

        elif isinstance(node, dag.Concat):

            node.isMPC = node.requiresMPC()

        elif isinstance(node, dag.Store):

            continue
        
        elif isinstance(node, dag.Create):
        
            continue

        else:

            assert(False)

def pushUpMPC(revSortedNodes):

    for node in revSortedNodes:
        
        # Apply operator-specific rules to pass collusion
        # groups from the output relation of an op-node to
        # its inputs
        node.backPropCollSets()

        # Update the node's MPC mode which might have changed
        # as a result of the collusion set propagation
        node.updateMPC()

def rewriteDag(dag):

    sortedNodes = dag.topSort()
    pushDownMPC(sortedNodes)
    # ironic?
    # pushUpMPC(sortedNodes[::-1])
    return str(dag)

def mpc(f):

    def wrap():
        beerProg = rewriteDag(dag.OpDag(f()))
        return beerProg

    return wrap
