from . import part
from salmon.dag import OpDag, Create
from salmon.codegen import scotch
from copy import deepcopy


def partDag(dag):

    sorted_nodes = dag.topSort()
    best = part.getBestPartition(sorted_nodes)

    return best


def heupart(dag):
    # hack hack hack

    def visit(node, collected, new_roots, mode):

        if node.isMPC == mode:
            collected.append(node)
            for child in node.children:
                visit(child, collected, new_roots, mode)
        else:
            if node not in new_roots:
                new_roots.append(node)

    def split_dag(current_dag, mode):

        collected = []
        new_roots = []

        # this will traverse into the current dag until all boundary nodes are
        # hit
        for root in current_dag.roots:
            visit(root, collected, new_roots, mode)

        for root in new_roots:
            # boundary nodes are guaranteed to be unary (open and close) so we
            # don't need to worry about multiple parents
            parent = root.parent
            if parent:
                parent.children.remove(root)
                # replace parent with create node of output relation
                create_op = Create(parent.outRel)
                # hack mpc flag
                create_op.isMPC = not mode 
                root.parent = create_op
                create_op.children.add(root)
            if root in current_dag.roots:
                current_dag.roots.remove(root)
        # new roots are parent create nodes we inserted
        parent_roots = [root.parent for root in new_roots]
        # return subdag below boundary nodes
        return OpDag(set(parent_roots))

    nextdag = dag
    mpcmode = False
    mapping = []

    while nextdag.roots:
        fmwk = "sharemind" if mpcmode else "spark"
        # store subdag
        mapping.append((fmwk, nextdag))
        # partition of next subdag
        nextdag = split_dag(nextdag, mpcmode)
        # flip mode
        mpcmode = not mpcmode

    return mapping
