from salmon.dag import Dag
from math import inf
from salmon.codegen import spark, sharemind


class SubDag(Dag):

    def __init__(self, nodes, name=''):
        self.nodes = nodes
        self.roots = self.findRoots()
        self.leaves = self.findLeaves()
        self.name = name
        super(SubDag, self).__init__(self.roots)

    def findLeaves(self):

        def isLocalLeaf(current_node, all_nodes):
            return len(set.intersection(current_node.children, set(all_nodes))) == 0

        leaves = []

        for node in self.nodes():
            if not node.isLeaf():
                if isLocalLeaf(node, self.nodes):
                    node.children = set()
                    leaves.append(node)

        return leaves

    def findRoots(self):

        def isLocalRoot(node, all_nodes):
            return len(set.intersection(node.parents, set(all_nodes))) == 0

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


# calls appropriate codegen for a given job
def mapToBackends(jobs):
    for job in jobs:
        if job[1] == 'spark':
            # TODO: make spark output dir configurable
            spark.SparkCodeGen(job[0]).generate(job[0].name, "/tmp")
        elif job[1] == 'sharemind':
            for i in range(1,4):
                sharemind.SharemindCodeGen(job[0], i).generate(
                    "{0}-{1}".format(job[0].name, i),
                    "/home/sharemind/Sharemind-SDK/sharemind/client")
        else:
            print("unknown backend for job {0}".format(job[0].name))

