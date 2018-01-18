"""
Workflow graph optimizations and transformations.
"""
import copy
import warnings

import conclave.dag as saldag
import conclave.lang as sal
import conclave.utils as utils


def push_op_node_down(top_node: saldag.OpNode, bottom_node: saldag.OpNode):
    """
    Pushes a node that must be done under MPC further down in the DAG,
    and inserts their child nodes that can be done locally above it.
    """

    # only dealing with one grandchild case for now
    assert (len(bottom_node.children) <= 1)
    child = next(iter(bottom_node.children), None)

    # remove bottom node between the bottom node's child
    # and the top node
    saldag.remove_between(top_node, child, bottom_node)

    # we need all parents of the parent node
    grand_parents = copy.copy(top_node.get_sorted_parents())

    # we will insert the removed bottom node between
    # each parent of the top node and the top node
    for idx, grand_parent in enumerate(grand_parents):
        to_insert = copy.deepcopy(bottom_node)
        to_insert.out_rel.rename(to_insert.out_rel.name + "_" + str(idx))
        to_insert.parents = set()
        to_insert.children = set()
        saldag.insert_between(grand_parent, top_node, to_insert)
        to_insert.update_stored_with()


def split_node(node: saldag.OpNode):
    """
    For now, deals with case where there is a Concat into an
    Aggregate. Here, local aggregation can be computed before
    concatenation, and then another aggregation under MPC.
    """

    # Only dealing with single child case for now
    assert (len(node.children) <= 1)
    clone = copy.deepcopy(node)
    clone.out_rel.rename(node.out_rel.name + "_obl")
    clone.parents = set()
    clone.children = set()
    clone.is_mpc = True
    child = next(iter(node.children), None)
    saldag.insert_between(node, child, clone)


# more than one child & is_boundary
def fork_node(node: saldag.Concat):
    """
    Concat nodes are often MPC boundaries. This method forks a Concat
    node that has more than one child node into a separate Concat node
    for each of it's children.
    """

    # we can skip the first child
    child_it = enumerate(copy.copy(node.get_sorted_children()))
    next(child_it)
    # clone node for each of the remaining children
    for idx, child in child_it:
        # create clone and rename output relation to
        # avoid identical relation names for different nodes
        clone = copy.deepcopy(node)
        clone.out_rel.rename(node.out_rel.name + "_" + str(idx))
        clone.parents = copy.copy(node.parents)
        warnings.warn("hacky fork_node")
        clone.ordered = copy.copy(node.ordered)
        clone.children = {child}
        for parent in clone.parents:
            parent.children.add(clone)
        node.children.remove(child)
        # make cloned node the child's new parent
        child.replace_parent(node, clone)
        child.update_op_specific_cols()


class DagRewriter:
    """ Top level DAG rewrite class. Traverses DAG, reorders nodes, and applies optimizations to certain nodes. """
    def __init__(self):

        # If true we visit topological ordering of condag in reverse
        self.reverse = False

    def rewrite(self, dag: saldag.OpDag):
        """ Traverse topologically sorted DAG, inspect each node. """
        ordered = dag.top_sort()
        if self.reverse:
            ordered = ordered[::-1]

        for node in ordered:
            print(type(self).__name__, "rewriting", node.out_rel.name)
            if isinstance(node, saldag.Aggregate):
                self._rewrite_aggregate(node)
            elif isinstance(node, saldag.Divide):
                self._rewrite_divide(node)
            elif isinstance(node, saldag.Project):
                self._rewrite_project(node)
            elif isinstance(node, saldag.Filter):
                self._rewrite_filter(node)
            elif isinstance(node, saldag.Multiply):
                self._rewrite_multiply(node)
            elif isinstance(node, saldag.RevealJoin):
                self._rewrite_reveal_join(node)
            elif isinstance(node, saldag.HybridJoin):
                self._rewrite_hybrid_join(node)
            elif isinstance(node, saldag.Join):
                self._rewrite_join(node)
            elif isinstance(node, saldag.Concat):
                self._rewrite_concat(node)
            elif isinstance(node, saldag.Close):
                self._rewrite_close(node)
            elif isinstance(node, saldag.Open):
                self._rewrite_open(node)
            elif isinstance(node, saldag.Create):
                self._rewrite_create(node)
            elif isinstance(node, saldag.Distinct):
                self._rewrite_distinct(node)
            else:
                msg = "Unknown class " + type(node).__name__
                raise Exception(msg)


class MPCPushDown(DagRewriter):
    """ DagRewriter subclass for pushing MPC boundaries down in workflows. """

    def __init__(self):
        """ Initialize MPCPushDown object. """

        super(MPCPushDown, self).__init__()

    def _do_commute(self, top_op: saldag.OpNode, bottom_op: saldag.OpNode):
        # TODO: over-simplified
        # TODO: add rules for other ops

        if isinstance(top_op, saldag.Aggregate):
            if isinstance(bottom_op, saldag.Divide):
                return True
            else:
                return False
        else:
            return False

    def _rewrite_default(self, node: saldag.OpNode):
        """
        Throughout the rewrite process, a node might switch from requiring
        MPC to no longer requiring it. This method updates a node's MPC
        status by calling an method on it from the OpNode class and it's
        subclasses.
        """

        node.is_mpc = node.requires_mpc()

    def _rewrite_unary_default(self, node: saldag.UnaryOpNode):
        """
        If the parent of a UnaryOpNode is a Concat or an Aggregation (which must be
        done under MPC), then the Concat or Aggregate operation can be pushed beneath
        it's child UnaryOpNode, which allows the operation at that node to be done
        outside of MPC. This method tests whether such a transformation can be performed.
        """
        parent = next(iter(node.parents))
        if parent.is_mpc:
            # if node is leaf stop
            if node.is_leaf():
                node.is_mpc = True
                return
            # node is not leaf
            if isinstance(parent, saldag.Concat) and parent.is_boundary():
                push_op_node_down(parent, node)
            elif isinstance(parent, saldag.Aggregate) and self._do_commute(parent, node):
                agg_op = parent
                agg_parent = agg_op.parent
                if isinstance(agg_parent, saldag.Concat) and agg_parent.is_boundary():
                    concat_op = agg_parent
                    assert len(concat_op.children) == 1
                    push_op_node_down(agg_op, node)
                    updated_node = agg_op.parent
                    push_op_node_down(concat_op, updated_node)
                else:
                    node.is_mpc = True
            else:
                node.is_mpc = True
        else:
            pass

    def _rewrite_aggregate(self, node: saldag.Aggregate):
        """ Aggregate specific pushdown logic. """

        parent = next(iter(node.parents))
        if parent.is_mpc:
            if isinstance(parent, saldag.Concat) and parent.is_boundary():
                split_node(node)
                push_op_node_down(parent, node)
            else:
                node.is_mpc = True
        else:
            pass

    def _rewrite_project(self, node: saldag.Project):

        self._rewrite_unary_default(node)

    def _rewrite_filter(self, node: saldag.Filter):

        self._rewrite_unary_default(node)

    def _rewrite_multiply(self, node: saldag.Multiply):

        self._rewrite_unary_default(node)

    def _rewrite_divide(self, node: saldag.Divide):

        self._rewrite_unary_default(node)

    def _rewrite_reveal_join(self, node: saldag.RevealJoin):

        raise Exception("RevealJoin encountered during MPCPushDown")

    def _rewrite_hybrid_join(self, node: saldag.HybridJoin):

        raise Exception("HybridJoin encountered during MPCPushDown")

    def _rewrite_join(self, node: saldag.Join):

        self._rewrite_default(node)

    def _rewrite_concat(self, node: saldag.Concat):
        """ Concat nodes with more than 1 child can be forked into multiple Concats. """

        if node.requires_mpc():
            node.is_mpc = True
            if len(node.children) > 1 and node.is_boundary():
                fork_node(node)

    def _rewrite_create(self, node: saldag.Create):

        pass


class MPCPushUp(DagRewriter):
    """ DagRewriter subclass for pushing MPC boundary up in workflows. """

    def __init__(self):
        """ Initialize MPCPushUp object. """

        super(MPCPushUp, self).__init__()
        self.reverse = True

    def _rewrite_unary_default(self, node: saldag.UnaryOpNode):
        """ If a UnaryOpNode is at a lower MPC boundary, it can be computed locally. """

        par = next(iter(node.parents))
        if node.is_reversible() and node.is_lower_boundary() and not par.is_root():
            print("lower boundary", node)
            node.get_in_rel().stored_with = copy.copy(node.out_rel.stored_with)
            node.is_mpc = False

    def _rewrite_aggregate(self, node: saldag.Aggregate):

        pass

    def _rewrite_divide(self, node: saldag.Divide):

        self._rewrite_unary_default(node)

    def _rewrite_project(self, node: saldag.Project):

        self._rewrite_unary_default(node)

    def _rewrite_filter(self, node: saldag.Filter):

        self._rewrite_unary_default(node)

    def _rewrite_multiply(self, node: saldag.Multiply):

        self._rewrite_unary_default(node)

    def _rewrite_reveal_join(self, node: saldag.RevealJoin):

        raise Exception("RevealJoin encountered during MPCPushUp")

    def _rewrite_hybrid_join(self, node: saldag.HybridJoin):

        raise Exception("HybridJoin encountered during MPCPushUp")

    def _rewrite_join(self, node: saldag.Join):

        pass

    def _rewrite_concat(self, node: saldag.Concat):
        """
        Concats are always reversible, only need to know if we are
        dealing with a boundary node (in which case it can be computed
        outside of MPC).
        """

        if node.is_lower_boundary():

            out_stored_with = node.out_rel.stored_with
            for par in node.parents:
                if not par.is_root():
                    par.out_rel.stored_with = copy.copy(out_stored_with)
            node.is_mpc = False

    def _rewrite_create(self, node: saldag.Create):

        pass


class CollSetPropDown(DagRewriter):

    def __init__(self):

        super(CollSetPropDown, self).__init__()

    def _rewrite_aggregate(self, node: [saldag.Aggregate, saldag.IndexAggregate]):

        in_group_cols = node.group_cols
        out_group_cols = node.out_rel.columns[:-1]
        for i in range(len(out_group_cols)):
            out_group_cols[i].coll_sets |= copy.deepcopy(in_group_cols[i].coll_sets)
        in_agg_col = node.agg_col
        out_agg_col = node.out_rel.columns[-1]
        out_agg_col.coll_sets |= copy.deepcopy(in_agg_col.coll_sets)

    def _rewrite_divide(self, node: saldag.Divide):

        out_rel_cols = node.out_rel.columns
        operands = node.operands
        target_col = node.target_col

        # Update target column collusion set
        target_col_out = out_rel_cols[target_col.idx]

        target_col_out.coll_sets |= utils.coll_sets_from_columns(operands)

        # The other columns weren't modified so the collusion sets
        # simply carry over
        for in_col, out_col in zip(node.get_in_rel().columns, out_rel_cols):
            if in_col != target_col:
                out_col.coll_sets |= copy.deepcopy(in_col.coll_sets)

    def _rewrite_project(self, node: saldag.Project):

        selected_cols = node.selected_cols

        for in_col, out_col in zip(selected_cols, node.out_rel.columns):
            out_col.coll_sets |= copy.deepcopy(in_col.coll_sets)

    def _rewrite_filter(self, node: saldag.Filter):

        out_rel_cols = node.out_rel.columns

        for in_col, out_col in zip(node.get_in_rel().columns, out_rel_cols):
            out_col.coll_sets |= copy.deepcopy(in_col.coll_sets)

    def _rewrite_multiply(self, node: saldag.Multiply):

        out_rel_cols = node.out_rel.columns
        operands = node.operands
        target_col = node.target_col

        # Update target column collusion set
        target_col_out = out_rel_cols[target_col.idx]

        target_col_out.coll_sets |= utils.coll_sets_from_columns(operands)

        # The other columns weren't modified so the collusion sets
        # simply carry over
        for in_col, out_col in zip(node.get_in_rel().columns, out_rel_cols):
            if in_col != target_col:
                out_col.coll_sets |= copy.deepcopy(in_col.coll_sets)

    def _rewrite_hybrid_join(self, node: saldag.HybridJoin):

        raise Exception("HybridJoin encountered during CollSetPropDown")

    def _rewrite_join(self, node: saldag.Join):

        left_in_rel = node.get_left_in_rel()
        right_in_rel = node.get_right_in_rel()

        left_join_cols = node.left_join_cols
        right_join_cols = node.right_join_cols

        num_join_cols = len(left_join_cols)

        out_join_cols = node.out_rel.columns[:num_join_cols]
        key_cols_coll_sets = []
        for i in range(len(left_join_cols)):
            key_cols_coll_sets.append(utils.merge_coll_sets(
                left_join_cols[i].coll_sets, right_join_cols[i].coll_sets))
            out_join_cols[i].coll_sets = key_cols_coll_sets[i]

        abs_idx = len(left_join_cols)
        for in_col in left_in_rel.columns:
            if in_col not in set(left_join_cols):
                for key_col_coll_sets in key_cols_coll_sets:
                    node.out_rel.columns[abs_idx].coll_sets = \
                        utils.merge_coll_sets(key_col_coll_sets, in_col.coll_sets)
                abs_idx += 1

        for in_col in right_in_rel.columns:
            if in_col not in set(right_join_cols):
                for key_col_coll_sets in key_cols_coll_sets:
                    node.out_rel.columns[abs_idx].coll_sets = \
                        utils.merge_coll_sets(key_col_coll_sets, in_col.coll_sets)
                abs_idx += 1

    def _rewrite_concat(self, node: saldag.Concat):

        # Copy over columns from existing relation
        out_rel_cols = node.out_rel.columns

        # Combine per-column collusion sets
        for idx, col in enumerate(out_rel_cols):
            columns_at_idx = [in_rel.columns[idx] for in_rel in node.get_in_rels()]
            col.coll_sets = utils.coll_sets_from_columns(columns_at_idx)

    def _rewrite_create(self, node: saldag.Create):

        pass


class HybridJoinOpt(DagRewriter):

    def __init__(self):

        super(HybridJoinOpt, self).__init__()

    def _rewrite_aggregate(self, node: saldag.Aggregate):

        pass

    def _rewrite_project(self, node: saldag.Project):

        pass

    def _rewrite_filter(self, node: saldag.Filter):

        pass

    def _rewrite_divide(self, node: saldag.Divide):

        pass

    def _rewrite_multiply(self, node: saldag.Multiply):

        pass

    def _rewrite_reveal_join(self, node: saldag.RevealJoin):

        # TODO
        pass

    def _rewrite_hybrid_join(self, node: saldag.HybridJoin):

        raise Exception("HybridJoin encountered during HybridJoinOpt")

    def _rewrite_join(self, node: saldag.Join):

        if node.is_mpc:
            out_rel = node.out_rel
            key_col_idx = 0
            # oversimplifying here. what if there are multiple singleton
            # coll_sets?
            singleton_coll_sets = filter(
                lambda s: len(s) == 1,
                out_rel.columns[key_col_idx].coll_sets)
            singleton_coll_sets = sorted(list(singleton_coll_sets))
            if singleton_coll_sets:
                trusted_party = next(iter(singleton_coll_sets[0]))
                hybrid_join_op = saldag.HybridJoin.from_join(node, trusted_party)
                parents = hybrid_join_op.parents
                for par in parents:
                    par.replace_child(node, hybrid_join_op)

    def _rewrite_concat(self, node: saldag.Concat):

        pass

    def _rewrite_create(self, node: saldag.Create):

        pass


class InsertOpenAndCloseOps(DagRewriter):
    # TODO: this class is messy

    def __init__(self):

        super(InsertOpenAndCloseOps, self).__init__()

    def _rewrite_default_unary(self, node: saldag.UnaryOpNode):

        # TODO: can there be a case when children have different stored_with sets?
        warnings.warn("hacky insert store ops")
        in_stored_with = node.get_in_rel().stored_with
        out_stored_with = node.out_rel.stored_with
        if in_stored_with != out_stored_with:
            if node.is_lower_boundary():
                # input is stored with one set of parties
                # but output must be stored with another so we
                # need an open operation
                out_rel = copy.deepcopy(node.out_rel)
                out_rel.rename(out_rel.name + "_open")
                # reset stored_with on parent so input matches output
                node.out_rel.stored_with = copy.copy(in_stored_with)

                # create and insert store node
                store_op = saldag.Open(out_rel, None)
                store_op.is_mpc = True
                saldag.insert_between_children(node, store_op)
            else:
                raise Exception(
                    "different stored_with on non-lower-boundary unary op", node)

    def _rewrite_aggregate(self, node: [saldag.Aggregate, saldag.IndexAggregate]):

        self._rewrite_default_unary(node)

    def _rewrite_divide(self, node: saldag.Divide):

        self._rewrite_default_unary(node)

    def _rewrite_project(self, node: saldag.Project):

        self._rewrite_default_unary(node)

    def _rewrite_filter(self, node: saldag.Filter):

        self._rewrite_default_unary(node)

    def _rewrite_multiply(self, node: saldag.Multiply):

        self._rewrite_default_unary(node)

    def _rewrite_hybrid_join(self, node: saldag.HybridJoin):

        self._rewrite_join(node)

    def _rewrite_join(self, node: saldag.Join):

        out_stored_with = node.out_rel.stored_with
        ordered_pars = [node.left_parent, node.right_parent]

        left_stored_with = node.get_left_in_rel().stored_with
        right_stored_with = node.get_right_in_rel().stored_with
        in_stored_with = left_stored_with | right_stored_with

        for parent in ordered_pars:
            if node.is_upper_boundary():
                # Entering mpc mode so need to secret-share before op
                out_rel = copy.deepcopy(parent.out_rel)
                out_rel.rename(out_rel.name + "_close")
                out_rel.stored_with = copy.copy(in_stored_with)
                # create and insert close node
                close_op = saldag.Close(out_rel, None)
                close_op.is_mpc = True
                saldag.insert_between(parent, node, close_op)
            # else:
            #     raise Exception(
            #         "different stored_with on non-upper-boundary join", node.debug_str())
        if node.is_leaf():
            if len(in_stored_with) > 1 and len(out_stored_with) == 1:
                target_party = next(iter(out_stored_with))
                node.out_rel.stored_with = copy.copy(in_stored_with)
                sal._open(node, node.out_rel.name + "_open", target_party)

    def _rewrite_concat(self, node: saldag.Concat):

        assert (not node.is_lower_boundary())

        out_stored_with = node.out_rel.stored_with
        ordered_pars = node.get_sorted_parents()
        for parent in ordered_pars:
            par_stored_with = parent.out_rel.stored_with
            if par_stored_with != out_stored_with:
                out_rel = copy.deepcopy(parent.out_rel)
                out_rel.rename(out_rel.name + "_close")
                out_rel.stored_with = copy.copy(out_stored_with)
                # create and insert close node
                store_op = saldag.Close(out_rel, None)
                store_op.is_mpc = True
                saldag.insert_between(parent, node, store_op)

    def _rewrite_create(self, node: saldag.Create):

        pass


class ExpandCompositeOps(DagRewriter):
    """Replaces operator nodes that correspond to composite operations
    (for example hybrid joins) into subdags of primitive operators"""

    def __init__(self):
        super(ExpandCompositeOps, self).__init__()

    def _rewrite_aggregate(self, node: [saldag.Aggregate, saldag.IndexAggregate]):
        pass

    def _rewrite_divide(self, node: saldag.Divide):
        pass

    def _rewrite_project(self, node: saldag.Project):
        pass

    def _rewrite_filter(self, node: saldag.Filter):
        pass

    def _rewrite_multiply(self, node: saldag.Multiply):
        pass

    def _rewrite_reveal_join(self, node: saldag.RevealJoin):
        pass

    def _rewrite_hybrid_join(self, node: saldag.HybridJoin):
        # TODO
        suffix = "rand"

        # in left parents' children, replace self with first primitive operator
        # in expanded subdag
        shuffled_a = sal.shuffle(node.left_parent, "shuffled_a")
        shuffled_a.is_mpc = True
        node.left_parent.children.remove(node)

        # same for right parent
        shuffled_b = sal.shuffle(node.right_parent, "shuffled_b")
        shuffled_b.is_mpc = True
        node.right_parent.children.remove(node)

        persisted_b = sal._persist(shuffled_b, "persisted_b")
        persisted_b.is_mpc = True
        persisted_a = sal._persist(shuffled_a, "persisted_a")
        persisted_a.is_mpc = True

        keys_a_closed = sal.project(shuffled_a, "keys_a_closed", ["a"])
        keys_a_closed.is_mpc = True
        keys_b_closed = sal.project(shuffled_b, "keys_b_closed", ["c"])
        keys_b_closed.is_mpc = True

        keys_a = sal._open(keys_a_closed, "keys_a", 1)
        keys_a.is_mpc = True
        keys_b = sal._open(keys_b_closed, "keys_b", 1)
        keys_b.is_mpc = True

        indexed_a = sal.index(keys_a, "indexed_a", "index_a")
        indexed_a.is_mpc = False

        indexed_b = sal.index(keys_b, "indexed_b", "index_b")
        indexed_b.is_mpc = False

        joined_indices = sal.join(
            indexed_a, indexed_b, "joined_indices", ["a"], ["c"])
        joined_indices.is_mpc = False

        indices_only = sal.project(
            joined_indices, "indices_only", ["index_a", "index_b"])
        indices_only.is_mpc = False

        # TODO: update stored_with to use union of parent out_rel stored_with sets
        indices_closed = sal._close(
            indices_only, "indices_closed", set([1, 2]))
        indices_closed.is_mpc = True

        joined = sal._index_join(persisted_a, persisted_b, "joined",
                                 ["a"], ["c"], indices_closed)
        joined.is_mpc = True

        # replace self with leaf of expanded subdag in each child node
        for child in node.get_sorted_children():
            child.replace_parent(node, joined)
        # add former children to children of leaf
        joined.children = node.children

    def _rewrite_join(self, node: saldag.Join):
        pass

    def _rewrite_concat(self, node: saldag.Concat):
        pass

    def _rewrite_create(self, node: saldag.Create):
        pass

    def _rewrite_open(self, node: saldag.Open):
        pass

    def _rewrite_close(self, node: saldag.Close):
        pass


def rewrite_dag(dag: saldag.OpDag):
    MPCPushDown().rewrite(dag)
    # ironic?
    MPCPushUp().rewrite(dag)
    CollSetPropDown().rewrite(dag)
    HybridJoinOpt().rewrite(dag)
    InsertOpenAndCloseOps().rewrite(dag)
    ExpandCompositeOps().rewrite(dag)
    return dag


def scotch(f: callable):
    from conclave.codegen import scotch
    from conclave import CodeGenConfig

    def wrap():
        code = scotch.ScotchCodeGen(CodeGenConfig(), f())._generate(None, None)
        return code

    return wrap


def sharemind(f: callable):
    from conclave.codegen import sharemind
    from conclave import CodeGenConfig

    def wrap():
        code = sharemind.SharemindCodeGen(CodeGenConfig(), f())._generate(None, None)
        return code

    return wrap


def mpc(*args):
    def _mpc(f):
        def wrapper(*args, **kwargs):
            dag = rewrite_dag(saldag.OpDag(f()))
            return dag

        return wrapper

    if len(args) == 1 and callable(args[0]):
        # No arguments, this is the decorator
        # Set default values for the arguments
        party = None
        return _mpc(args[0])
    else:
        # This is just returning the decorator
        party = args[0]
        return _mpc


def dag_only(f: callable):
    def wrap():
        return saldag.OpDag(f())

    return wrap
