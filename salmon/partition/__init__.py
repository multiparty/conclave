from . import part


def partitionDag(dag):
    sorted_nodes = dag.topSort()
    all_partitions = part.getAllPartitions(sorted_nodes)

    all_jobs = []
    for partition in all_partitions:
        p = []
        for subdag in partition:
            # TODO: naming
            p.append(part.SubDag(subdag))
        all_jobs.append(p)

    current_min = 0
    best = ''
    for p in all_jobs:
        cost = part.measureCost(p)
        if cost < current_min:
            best = p


    res = []
    for subdag in best:
        backend = part.determineBackend(subdag)
        res.append((subdag, backend))

    return res

