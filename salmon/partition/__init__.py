from . import part
from salmon.dag import OpDag, Create, Close, Open, UnaryOpNode, Index
from salmon.codegen import scotch
from copy import copy, deepcopy


def partDag(dag):

    sorted_nodes = dag.topSort()
    best = part.getBestPartition(sorted_nodes)

    return best


def heupart(dag):

    def is_correct_mode(node, mode, available):

        # if node itself is in other mode return false
        if node.isMPC != mode:
            return False

        # otherwise check parents
        return node.parents.issubset(available) or not (node.parents or available)

    def split_dag(current_dag, available):

        # need topological ordering
        ordered = current_dag.topSort()
        # first node determines if we're in mpc mode or not
        mode = ordered[0].isMPC

        # available = set()
        # can new roots be set?
        new_roots = []

        # traverse current dag until all boundary nodes are hit
        for node in ordered:
            print(node)
            if is_correct_mode(node, mode, available):
                print("available")
                available.add(node)
            elif ((not node.parents) or (node.parents & available)):
                print("new root")
                if node not in new_roots:
                    new_roots.append(node)
            else:
                print("not available")
                pass

        print("new_roots", [str(root) for root in new_roots])

        for root in new_roots:
            for parent in copy(root.parents):
                if parent in available:
                    parent.children.remove(root)
                    # replace parent with create node of output relation
                    create_op = Create(parent.outRel)
                    # create op is in same mode as root
                    create_op.isMPC = root.isMPC
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

        print("parent_roots", [str(root) for root in parent_roots])
        
        return OpDag(set(parent_roots)), available

    def _merge_dags(left_dag, right_dag):

        # to merge, we only need to combine roots
        roots = left_dag.roots.union(right_dag.roots)
        return OpDag(roots)

    def merge_neighbor_dags(mapping):

        updated_mapping = []
        prev_fmwk, prev_subdag = None, None
        for fmwk, subdag in mapping:
            # we can merge neighboring subdags if they're mapped to the same
            # framework
            if fmwk == prev_fmwk:
                # merge dags together
                merged_dag = _merge_dags(prev_subdag, subdag)
                # pop previous subdag
                updated_mapping = updated_mapping[:-1]
                updated_mapping.append((fmwk, merged_dag))
            else:
                # can't merge, so just add subdag to result
                updated_mapping.append((fmwk, subdag))
            # keep track of previous values
            prev_fmwk = fmwk
            prev_subdag = subdag 
        return updated_mapping

    nextdag = dag
    mapping = []
    available = set()
    counter = 0

    while nextdag.roots:
        # determine if next dag is MPC or local
        mpcmode = nextdag.topSort()[0].isMPC
        # map to framework
        fmwk = "sharemind" if mpcmode else "spark"
        # store subdag
        mapping.append((fmwk, nextdag))
        # partition next subdag
        nextdag, available = split_dag(nextdag, available)
        counter += 1

    merged = merge_neighbor_dags(mapping)
    return merged
