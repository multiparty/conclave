from . import part


def partDag(dag):

    sorted_nodes = dag.topSort()
    best = part.getBestPartition(sorted_nodes)

    return best

def heuristic_partition(dag):

    def visit(node, collected, new_roots, mode):

        if node.isMPC == mode:
            collected.append(node)
            for child in node.children:
                visit(child, collected, new_roots, mode)
        else:
            # boundary nodes are guaranteed to be unary
            # (open and close) so we don't need to worry
            # about multiple parents
            node.makeOrphan()
            new_roots.append(node)

    collected = []
    new_roots = []

    for root in dag.roots:
        visit(root, collected, new_roots, False)

    print(collected)
    print(new_roots)

    return [dag]

