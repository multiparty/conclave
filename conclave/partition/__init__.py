from copy import copy, deepcopy

from conclave.codegen.scotch import ScotchCodeGen
from conclave.config import CodeGenConfig
from conclave.dag import OpDag, Dag, Create, Open, Persist, OpNode


def heupart(dag: Dag, mpc_frameworks: list, local_frameworks: list):
    """ Non-exhaustive partition. Returns best partition with respect to certain heuristics. """

    def get_stored_with(node: OpNode):
        """ Returns stored_with set of out_rel or in_rel of a node, depending on it's type. """

        if isinstance(node, Open):
            return node.get_in_rel().stored_with
        elif isinstance(node, Create):
            return get_stored_with(next(iter(node.children)))
        else:
            return node.out_rel.stored_with

    def is_correct_mode(node: OpNode, available: set, stored_with: set):
        """ Verifies that node is stored with same set of parties passed to this function. """

        if get_stored_with(node) != stored_with:
            return False

        # otherwise check parents
        return node.parents.issubset(available) or not (node.parents or available)

    def can_partition(dag: Dag, stored_with: set, top_available: set):
        """ Returns whether the Dag passed to it can be partitioned. """

        # copy so we don't overwrite global available nodes in this pass
        available = deepcopy(top_available)
        ordered = dag.top_sort()
        unavailable = set()

        for node in ordered:
            if node in unavailable and get_stored_with(node) == stored_with:
                for parent in node.parents:
                    if parent in available and not isinstance(parent, Persist):
                        return False
            if is_correct_mode(node, available, stored_with):
                available.add(node)
            else:
                # mark all descendants as unavailable
                descendants = Dag({node}).get_all_nodes()
                unavailable = unavailable.union(descendants)
        return True

    def disconnect_at_roots(current_dag: Dag, available: set, new_roots: list):

        previous_parents = set()
        create_op_lookup = dict()
        for root in new_roots:
            for parent in copy(root.parents):
                if parent in available:
                    create_op = None
                    if parent not in previous_parents:
                        create_op = Create(deepcopy(parent.out_rel))
                        # create op is in same mode as root
                        create_op.is_mpc = root.is_mpc
                        previous_parents.add(parent)
                        create_op_lookup[parent.out_rel.name] = create_op
                    else:
                        create_op = create_op_lookup[parent.out_rel.name]
                    # unlink root from parent
                    parent.children.remove(root)
                    # insert create op between parent and root
                    root.replace_parent(parent, create_op)
                    # connect create op with root
                    create_op.children.add(root)
                    # keep track of parents we have already visited
                    previous_parents.add(parent)
                    create_op_lookup[create_op.out_rel.name] = create_op
            if root in current_dag.roots:
                current_dag.roots.remove(root)

        parent_roots = set().union(*[root.parents for root in new_roots])
        for root in new_roots:
            if isinstance(root, Create):
                parent_roots.add(root)

        return OpDag(set(parent_roots)), available

    def find_new_roots(current_dag: Dag, available: set, stored_with: set):

        # need topological ordering
        ordered = current_dag.top_sort()

        # roots of the next subdag, i.e., where the current subdag will end
        new_roots = []

        # traverse current condag until all boundary nodes are hit
        for node in ordered:
            if is_correct_mode(node, available, stored_with):
                available.add(node)
            elif (not node.parents) or (node.parents & available):
                if node not in new_roots:
                    new_roots.append(node)

        # roots of the next subdag
        return new_roots

    def next_partition(nextdag, available, holding_parties):

        # roots of the next subdag
        new_roots = find_new_roots(nextdag, available, holding_parties)
        # disconnect current dags at new root nodes and return the disconnected
        # bottom condag
        return disconnect_at_roots(nextdag, available, new_roots)

    def _merge_dags(left_dag, right_dag):

        # TODO: should go inside dagutils, once dagutils exists
        # to merge, we only need to combine roots
        roots = left_dag.roots.union(right_dag.roots)
        return OpDag(roots)

    def next_holding_ps(nextdag, available):

        roots = nextdag.roots
        for root in sorted(roots, key=lambda node: node.out_rel.name):
            holding_ps = get_stored_with(root)
            if can_partition(nextdag, holding_ps, available):
                return holding_ps, len(holding_ps) > 1
        raise Exception("Found no roots to partition on")

    def merge_neighbor_dags(mapping):

        updated_mapping = []
        prev_fmwk, prev_subdag, stored_with = None, None, None

        for fmwk, subdag, stored_with in mapping:
            # we can merge neighboring subdags if they're mapped to the same
            # framework and are stored by same parties
            # TODO this looks like a bug -- stored_with == prev_fmwk should never be true
            if fmwk == prev_fmwk and stored_with == prev_fmwk:
                # merge dags together
                merged_dag = _merge_dags(prev_subdag, subdag)
                # pop previous subdag
                updated_mapping = updated_mapping[:-1]
                updated_mapping.append((fmwk, merged_dag, stored_with))
            else:
                # can't merge, so just add subdag to result
                updated_mapping.append((fmwk, subdag, stored_with))
            # keep track of previous values
            prev_fmwk = fmwk
            prev_subdag = subdag
        return updated_mapping

    assert len(mpc_frameworks) == 1 and len(local_frameworks) == 1
    nextdag = dag
    mapping = []
    available = set()

    iterations = 0
    iteration_limit = 100

    local_fmwk = local_frameworks[0]
    mpc_fmwk = mpc_frameworks[0]

    print("##################")
    print(ScotchCodeGen(CodeGenConfig(), nextdag)._generate(0, 0))
    print("##################")

    while nextdag.roots:
        if iterations > iteration_limit:
            raise Exception("Reached iteration limit while partitioning")
        # find holding set and mpc mode of next valid partition
        holding_ps, mpcmode = next_holding_ps(nextdag, available)
        # select framework
        fmwk = mpc_fmwk if mpcmode else local_fmwk
        # store mapping
        mapping.append((fmwk, nextdag, holding_ps))
        # partition next subdag
        nextdag, available = next_partition(nextdag, available, holding_ps)
        # increment iteration count
        iterations += 1

    for fmwk, subdag, stored_with in mapping:
        print(fmwk, stored_with, ScotchCodeGen(CodeGenConfig(), subdag)._generate(0, 0))

    merged = merge_neighbor_dags(mapping)
    return merged
