from . import part

def partDag(dag):
    sorted_nodes = dag.topSort()
    best = part.getBestPartition(sorted_nodes)

    return best
