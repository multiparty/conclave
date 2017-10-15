from . import part
from salmon.dag import OpDag, Create, Close, Open, UnaryOpNode
from salmon.codegen import scotch
from copy import copy, deepcopy
from salmon.codegen.scotch import ScotchCodeGen
from salmon.codegen import CodeGenConfig


def partDag(dag):

    sorted_nodes = dag.topSort()
    best = part.getBestPartition(sorted_nodes)

    return best


def heupart(dag, mpc_frameworks, local_frameworks):

    def get_stored_with(node):

        if isinstance(node, Open):
            return node.getInRel().storedWith
        elif isinstance(node, Create):
            return get_stored_with(next(iter(node.children)))
        else:
            return node.outRel.storedWith

    def is_correct_mode(node, available, storedWith):

        # if node itself is stored with different set of parties it doesn't
        # belong inside current dag
        if get_stored_with(node) != storedWith:
            return False
        
        # otherwise check parents
        return node.parents.issubset(available) or not (node.parents or available)

    def split_dag(current_dag, available):

        # need topological ordering
        ordered = current_dag.topSort()
        # first node must always be create node
        assert isinstance(ordered[0], Create)
        # first node determines if we're in mpc mode or not        
        storedWith = get_stored_with(ordered[0])

        # available = set()
        # can new roots be set?
        new_roots = []

        # traverse current dag until all boundary nodes are hit
        for node in ordered:
            if is_correct_mode(node, available, storedWith):
                available.add(node)
            elif ((not node.parents) or (node.parents & available)):
                if node not in new_roots:
                    new_roots.append(node)
            else:
                pass

        for root in new_roots:
            for parent in copy(root.parents):
                if parent in available:
                    parent.children.remove(root)
                    # replace parent with create node of output relation
                    create_op = Create(deepcopy(parent.outRel))
                    # create op is in same mode as root
                    create_op.isMPC = root.isMPC
                    # create_op.outRel.storedWith = deepcopy(
                    #     root.outRel.storedWith)
                    # insert create op between parent and root
                    root.replaceParent(parent, create_op)
                    # connect create op with root
                    create_op.children.add(root)
            if root in current_dag.roots:
                current_dag.roots.remove(root)

        parent_roots = set().union(*[root.parents for root in new_roots])
        for root in new_roots:
            if isinstance(root, Create):
                parent_roots.add(root)

        return OpDag(set(parent_roots)), available

    def _merge_dags(left_dag, right_dag):

        # to merge, we only need to combine roots
        roots = left_dag.roots.union(right_dag.roots)
        return OpDag(roots)

    def merge_neighbor_dags(mapping):

        updated_mapping = []
        prev_fmwk, prev_subdag, stored_with = None, None, None
        for fmwk, subdag, stored_with in mapping:
            # we can merge neighboring subdags if they're mapped to the same
            # framework and are stored by same parties
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

    local_fmwk = local_frameworks[0]
    mpc_fmwk = mpc_frameworks[0]

    while nextdag.roots:
        first = nextdag.topSort()[0]
        assert isinstance(first, Create)
        storedWith = get_stored_with(first)
        mpcmode = first.isMPC
        # map to framework
        fmwk = mpc_fmwk if mpcmode else local_fmwk
        # store subdag
        mapping.append((fmwk, nextdag, storedWith))
        # partition next subdag
        nextdag, available = split_dag(nextdag, available)

    # for fmwk, subdag, storedWith in mapping:
    #     print(ScotchCodeGen(CodeGenConfig(), sd)._generate(0, 0))

    merged = merge_neighbor_dags(mapping)
    return merged
