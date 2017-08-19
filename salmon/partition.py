from salmon.dag import Dag
from math import inf

# TODO: (ben) helper method that takes best partition of subdags,
# reads in backend type strings, and calls appropriate code gen


class SubDag(Dag):

    def __init__(self, nodes):
        self.nodes = nodes
        self.roots = self.findRoots()
        self.leaves = self.findLeaves()
        super(SubDag, self).__init__(self.roots)

    def findLeaves(self):

        def isLocalLeaf(current_node, all_nodes):
            return len(set.intersection(current_node.children, set(all_nodes))) > 0

        leaves = []

        for node in self.nodes():
            if not node.isLeaf():
                if isLocalLeaf(node, self.nodes):
                    node.children = set()
                    leaves.append(node)

        return leaves

    def findRoots(self):

        def isLocalRoot(node, all_nodes):
            return len(set.intersection(node.parents, set(all_nodes))) > 0

        roots = []

        for node in self.nodes:
            if not node.isRoot():
                if isLocalRoot(node, self.nodes):
                    roots.append(node)

        return roots

    # return infinity if there exist incompatible collsets across nodes
    def getCost(self):
        return inf


# assuming that each partition will be a list of lists
def getAllPartitions(dag):
    return []


def measureCost(partition):
    cost = 0
    for job in partition:
        cost += job.getCost(job)
    return cost


# TODO: distinguish btwn sharemind and viff jobs
def determineBackend(subdag):
    nodes = subdag.nodes
    coll_sets = set()
    for node in nodes:
        for col in node.columns:
            coll_sets.add(col.collSets)
    if len(coll_sets) > 1:
        return 'sharemind'
    else:
        return 'spark'


# method that will get called on each dag object. return
# a list of jobs which represents and optimal partition.
def partitionDag(dag):
    sorted_nodes = dag.topSort()
    all_partitions = getAllPartitions(sorted_nodes)

    all_jobs = []
    for partition in all_partitions:
        part = []
        for subdag in partition:
            part.append(SubDag(subdag))
        all_jobs.append(part)

    current_min = 0
    best_partition = ""
    for part in all_jobs:
        cost = measureCost(part)
        if cost < current_min:
            best_partition = part

    res = []
    for subdag in best_partition:
        backend = determineBackend(subdag)
        res.append((subdag, backend))

    return res
