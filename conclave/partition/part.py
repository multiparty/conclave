from conclave.dag import *
try:
    from math import inf
except:
    # No inf until 3.5
    inf = float("inf")
from math import pow
from conclave.codegen import spark, sharemind
from sys import maxsize


class SubDag(Dag):

    def __init__(self, nodes):
        self.nodes = nodes
        self.roots = self.findRoots()
        self.leaves = self.findLeaves()
        # change naming in the future
        self.name = self.debugStr()
        super(SubDag, self).__init__(self.roots)

    def findLeaves(self):

        def isLocalLeaf(current_node, all_nodes):
            return len(set.intersection(current_node.children, set(all_nodes))) == 0

        leaves = []

        for node in self.nodes:
            if not node.is_leaf():
                if isLocalLeaf(node, self.nodes):
                    node.children = set()
                    leaves.append(node)

        return leaves

    def findRoots(self):

        def isLocalRoot(current_node, all_nodes):
            return len(set.intersection(current_node.parents, set(all_nodes))) == 0

        roots = []

        for node in self.nodes:
            if not node.is_root():
                if isLocalRoot(node, self.nodes):
                    roots.append(node)

        return roots

    # could be helpful later if we want to give estimates
    # of a workflow given a list of subdags
    def getCost(self):
        return inf

    def debugStr(self):
        return "{" + ", ".join([node.out_rel.name for node in self.nodes]) + "}"


def get_best_partition(nodes):
    # TODO: is this the best way to express maximum cost?
    max_cost = 1000
    num_ops = len(nodes)
    max_ops = int(pow(2, num_ops))
    cost = [[False for i in range(max_ops)] for j in range(max_cost)]
    cost[0][0] = True
    parent = [[0 for i in range(max_ops)] for j in range(max_cost)]
    parent_cost = [[0 for i in range(max_ops)] for j in range(max_cost)]
    scheduled_fmwk = [["spark" for i in range(
        max_ops)] for j in range(max_cost)]

    min_cost = [inf for i in range(max_ops)]
    job_cost = [inf for i in range(max_ops)]
    min_fmwk = ['' for i in range(max_ops)]
    min_cost[0] = 0
    job_cost[0] = 0

    all_ops_flag = max_ops - 1

    # TODO: this is a hacky way to iterate over frameworks
    fmwks = ['spark', 'sharemind']
    for i in range(1, max_ops):
        merge_nodes = []
        for j in range(num_ops):
            if (1 << j) & i:
                merge_nodes.append(nodes[j])
        # iterate over frameworks to find one of least cost,
        # w/r/t a selection of merged nodes
        for fmwk in fmwks:
            this_cost = measureCost(merge_nodes, fmwk)
            if this_cost < min_cost[i] and this_cost < max_cost:
                min_cost[i] = this_cost
                job_cost[i] = this_cost
                min_fmwk[i] = fmwk
    for cur_cost in range(max_cost):
        # flag to indicate when best solution is reached
        if cost[cur_cost][all_ops_flag]:
            break
        for jobs_exec in range(all_ops_flag):
            if cost[cur_cost][jobs_exec] and cur_cost <= min_cost[jobs_exec]:
                for jobs_to_merge in range(jobs_exec + 1, all_ops_flag):
                    jobs_merged = (jobs_to_merge & jobs_exec) ^ jobs_to_merge
                    next_jobs_exec = jobs_merged | jobs_exec
                    next_cost = cur_cost + job_cost[jobs_merged]
                    if (cost[next_cost][next_jobs_exec] is False) and next_cost < max_cost:
                        cost[next_cost][next_jobs_exec] = True
                        parent[next_cost][next_jobs_exec] = jobs_exec
                        parent_cost[next_cost][next_jobs_exec] = cur_cost
                        if next_cost < min_cost[next_jobs_exec]:
                            min_cost[next_jobs_exec] = next_cost
                            min_fmwk[next_jobs_exec] = min_fmwk[jobs_merged]
                        scheduled_fmwk[next_cost][
                            next_jobs_exec] = min_fmwk[jobs_merged]
    result = []

    cur_jobs_exec = all_ops_flag
    while cur_jobs_exec > 0:
        subdag = []
        assert cur_cost <= max_cost, \
            "At least one operator could not be mapped to a backend."
        # assert parent[final_cost] is not None
        prev_jobs_exec = parent[cur_cost][cur_jobs_exec]
        jobs_merged = prev_jobs_exec ^ cur_jobs_exec
        for num_op in range(num_ops):
            if int(pow(2, num_op)) & jobs_merged:
                subdag.append(nodes[num_op])
        result.append((SubDag(subdag), scheduled_fmwk[
                      cur_cost][cur_jobs_exec]))
        tmp_jobs_exec = cur_jobs_exec
        cur_jobs_exec = parent[cur_cost][cur_jobs_exec]
        cur_cost = parent_cost[cur_cost][tmp_jobs_exec]
    result.reverse()

    return result


# TODO: the way this is filled in is just for temporary testing
def measureCost(nodes, fmwk):
    cost = 0
    if fmwk == 'spark':
        for node in nodes:
            if node.isMPC:
                # can't use inf bc it tries to index the job_cost list at inf
                cost += 1000
            else:
                if isinstance(node, Aggregate):
                    cost += 1
                elif isinstance(node, Concat):
                    cost += 1
                elif isinstance(node, Create):
                    cost += 1
                elif isinstance(node, Join):
                    cost += 1
                elif isinstance(node, Project):
                    cost += 1
                elif isinstance(node, Multiply):
                    cost += 1
                elif isinstance(node, Divide):
                    cost += 1
                else:
                    print("encountered unknown operator type", repr(node))
    elif fmwk == 'sharemind':
        for node in nodes:
            if isinstance(node, Aggregate):
                cost += 10
            elif isinstance(node, Concat):
                cost += 10
            elif isinstance(node, Create):
                cost += 10
            elif isinstance(node, Close):
                cost += 10
            elif isinstance(node, PublicJoin):
                cost += 10
            elif isinstance(node, HybridJoin):
                cost += 10
            elif isinstance(node, Join):
                cost += 10
            elif isinstance(node, Open):
                cost += 10
            elif isinstance(node, Project):
                cost += 10
            elif isinstance(node, Multiply):
                cost += 10
            elif isinstance(node, Divide):
                cost += 10
            else:
                print("encountered unknown operator type", repr(node))
    return cost


# calls appropriate codegen for a given job
def mapToBackends(jobs):
    for job in jobs:
        if job[1] == 'spark':
            # TODO: make spark output dir configurable
            spark.SparkCodeGen(job[0]).generate(job[0].name, "/tmp")
        elif job[1] == 'sharemind':
            for i in range(1, 4):
                sharemind.SharemindCodeGen(job[0], i).generate(
                    "{0}-{1}".format(job[0].name, i),
                    "/home/sharemind/Sharemind-SDK/sharemind/client")
        else:
            print("unknown backend for job {0}".format(job[0].name))
