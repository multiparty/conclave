"""
Workflow graph optimizations and transformations.
"""
import copy
import warnings

import conclave.dag as ccdag
import conclave.lang as cc
import conclave.utils as utils


def push_op_node_down(top_node: ccdag.OpNode, bottom_node: ccdag.OpNode):
    """
    Pushes a node that must be done under MPC further down in the DAG,
    and inserts their child nodes that can be done locally above it.
    """

    # only dealing with one grandchild case for now
    assert (len(bottom_node.children) <= 1)
    child = next(iter(bottom_node.children), None)

    # remove bottom node between the bottom node's child
    # and the top node
    ccdag.remove_between(top_node, child, bottom_node)

    # we need all parents of the parent node
    grand_parents = copy.copy(top_node.get_sorted_parents())

    # we will insert the removed bottom node between
    # each parent of the top node and the top node
    for idx, grand_parent in enumerate(grand_parents):
        to_insert = copy.deepcopy(bottom_node)
        to_insert.out_rel.rename(to_insert.out_rel.name + "_" + str(idx))
        to_insert.parents = set()
        to_insert.children = set()
        ccdag.insert_between(grand_parent, top_node, to_insert)
        to_insert.update_stored_with()


def split_node(node: ccdag.OpNode):
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
    ccdag.insert_between(node, child, clone)


# more than one child & is_boundary
def fork_node(node: ccdag.Concat):
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

    def rewrite(self, dag: ccdag.OpDag):
        """ Traverse topologically sorted DAG, inspect each node. """
        ordered = dag.top_sort()
        if self.reverse:
            ordered = ordered[::-1]

        for node in ordered:
            print(type(self).__name__, "rewriting", node.out_rel.name)
            if isinstance(node, ccdag.HybridAggregate):
                self._rewrite_hybrid_aggregate(node)
            elif isinstance(node, ccdag.Aggregate):
                self._rewrite_aggregate(node)
            elif isinstance(node, ccdag.Divide):
                self._rewrite_divide(node)
            elif isinstance(node, ccdag.Project):
                self._rewrite_project(node)
            elif isinstance(node, ccdag.Filter):
                self._rewrite_filter(node)
            elif isinstance(node, ccdag.Multiply):
                self._rewrite_multiply(node)
            elif isinstance(node, ccdag.JoinFlags):
                self._rewrite_join_flags(node)
            elif isinstance(node, ccdag.RevealJoin):
                self._rewrite_reveal_join(node)
            elif isinstance(node, ccdag.HybridJoin):
                self._rewrite_hybrid_join(node)
            elif isinstance(node, ccdag.Join):
                self._rewrite_join(node)
            elif isinstance(node, ccdag.Concat):
                self._rewrite_concat(node)
            elif isinstance(node, ccdag.Close):
                self._rewrite_close(node)
            elif isinstance(node, ccdag.Open):
                self._rewrite_open(node)
            elif isinstance(node, ccdag.Create):
                self._rewrite_create(node)
            elif isinstance(node, ccdag.Distinct):
                self._rewrite_distinct(node)
            elif isinstance(node, ccdag.DistinctCount):
                self._rewrite_distinct_count(node)
            else:
                msg = "Unknown class " + type(node).__name__
                raise Exception(msg)

    def _rewrite_aggregate(self, node: ccdag.Aggregate):
        pass

    def _rewrite_hybrid_aggregate(self, node: ccdag.HybridAggregate):
        pass

    def _rewrite_divide(self, node: ccdag.Divide):
        pass

    def _rewrite_project(self, node: ccdag.Project):
        pass

    def _rewrite_filter(self, node: ccdag.Filter):
        pass

    def _rewrite_multiply(self, node: ccdag.Multiply):
        pass

    def _rewrite_join_flags(self, node: ccdag.JoinFlags):
        pass

    def _rewrite_reveal_join(self, node: ccdag.RevealJoin):
        pass

    def _rewrite_hybrid_join(self, node: ccdag.HybridJoin):
        pass

    def _rewrite_join(self, node: ccdag.Join):
        pass

    def _rewrite_concat(self, node: ccdag.Concat):
        pass

    def _rewrite_close(self, node: ccdag.Close):
        pass

    def _rewrite_open(self, node: ccdag.Open):
        pass

    def _rewrite_create(self, node: ccdag.Create):
        pass

    def _rewrite_distinct(self, node: ccdag.Distinct):
        pass

    def _rewrite_distinct_count(self, node: ccdag.DistinctCount):
        pass


class MPCPushDown(DagRewriter):
    """ DagRewriter subclass for pushing MPC boundaries down in workflows. """

    def __init__(self):
        """ Initialize MPCPushDown object. """

        super(MPCPushDown, self).__init__()

    def _do_commute(self, top_op: ccdag.OpNode, bottom_op: ccdag.OpNode):
        # TODO: over-simplified
        # TODO: add rules for other ops

        if isinstance(top_op, ccdag.Aggregate):
            if isinstance(bottom_op, ccdag.Divide):
                return True
            else:
                return False
        else:
            return False

    def _rewrite_default(self, node: ccdag.OpNode):
        """
        Throughout the rewrite process, a node might switch from requiring
        MPC to no longer requiring it. This method updates a node's MPC
        status by calling an method on it from the OpNode class and it's
        subclasses.
        """

        node.is_mpc = node.requires_mpc()

    def _rewrite_unary_default(self, node: ccdag.UnaryOpNode):
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
            if isinstance(parent, ccdag.Concat) and parent.is_boundary():
                push_op_node_down(parent, node)
            elif isinstance(parent, ccdag.Aggregate) and self._do_commute(parent, node):
                agg_op = parent
                agg_parent = agg_op.parent
                if isinstance(agg_parent, ccdag.Concat) and agg_parent.is_boundary():
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

    def _rewrite_aggregate(self, node: ccdag.Aggregate):
        """ Aggregate specific pushdown logic. """

        parent = next(iter(node.parents))
        if parent.is_mpc:
            if isinstance(parent, ccdag.Concat) and parent.is_boundary():
                split_node(node)
                push_op_node_down(parent, node)
            else:
                node.is_mpc = True
        else:
            pass

    def _rewrite_project(self, node: ccdag.Project):

        self._rewrite_unary_default(node)

    def _rewrite_filter(self, node: ccdag.Filter):

        self._rewrite_unary_default(node)

    def _rewrite_multiply(self, node: ccdag.Multiply):

        self._rewrite_unary_default(node)

    def _rewrite_divide(self, node: ccdag.Divide):

        self._rewrite_unary_default(node)

    def _rewrite_reveal_join(self, node: ccdag.RevealJoin):

        raise Exception("RevealJoin encountered during MPCPushDown")

    def _rewrite_hybrid_join(self, node: ccdag.HybridJoin):

        raise Exception("HybridJoin encountered during MPCPushDown")

    def _rewrite_join(self, node: ccdag.Join):

        self._rewrite_default(node)

    def _rewrite_concat(self, node: ccdag.Concat):
        """ Concat nodes with more than 1 child can be forked into multiple Concats. """

        if node.requires_mpc():
            node.is_mpc = True
            if len(node.children) > 1 and node.is_boundary():
                fork_node(node)

    def _rewrite_distinct_count(self, node: ccdag.DistinctCount):
        self._rewrite_unary_default(node)

class MPCPushUp(DagRewriter):
    """ DagRewriter subclass for pushing MPC boundary up in workflows. """

    def __init__(self):
        """ Initialize MPCPushUp object. """

        super(MPCPushUp, self).__init__()
        self.reverse = True

    def _rewrite_unary_default(self, node: ccdag.UnaryOpNode):
        """ If a UnaryOpNode is at a lower MPC boundary, it can be computed locally. """

        par = next(iter(node.parents))
        if node.is_reversible() and node.is_lower_boundary() and not par.is_root():
            node.get_in_rel().stored_with = copy.copy(node.out_rel.stored_with)
            node.is_mpc = False

    def _rewrite_divide(self, node: ccdag.Divide):

        self._rewrite_unary_default(node)

    def _rewrite_project(self, node: ccdag.Project):

        self._rewrite_unary_default(node)

    def _rewrite_filter(self, node: ccdag.Filter):

        self._rewrite_unary_default(node)

    def _rewrite_multiply(self, node: ccdag.Multiply):

        self._rewrite_unary_default(node)

    def _rewrite_reveal_join(self, node: ccdag.RevealJoin):

        raise Exception("RevealJoin encountered during MPCPushUp")

    def _rewrite_hybrid_join(self, node: ccdag.HybridJoin):

        raise Exception("HybridJoin encountered during MPCPushUp")

    def _rewrite_concat(self, node: ccdag.Concat):
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

    def _rewrite_create(self, node: ccdag.Create):

        pass


class CollSetPropDown(DagRewriter):
    """
    Pushes collusion sets down through the DAG. Collusion sets are
    column-specific and thus more granular than stored_with sets,
    which are defined over whole relations.
    """

    def __init__(self):

        super(CollSetPropDown, self).__init__()

    def _rewrite_aggregate(self, node: [ccdag.Aggregate, ccdag.IndexAggregate]):
        """ Push down collusion sets for an Aggregate or IndexAggregate node. """

        in_group_cols = node.group_cols
        out_group_cols = node.out_rel.columns[:-1]
        for i in range(len(out_group_cols)):
            out_group_cols[i].coll_sets |= copy.deepcopy(in_group_cols[i].coll_sets)
        in_agg_col = node.agg_col
        out_agg_col = node.out_rel.columns[-1]
        out_agg_col.coll_sets |= copy.deepcopy(in_agg_col.coll_sets)

    def _rewrite_divide(self, node: ccdag.Divide):
        """ Push down collusion sets for a Divide node. """

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

    def _rewrite_project(self, node: ccdag.Project):
        """ Push down collusion sets for a Project node. """

        selected_cols = node.selected_cols

        for in_col, out_col in zip(selected_cols, node.out_rel.columns):
            out_col.coll_sets |= copy.deepcopy(in_col.coll_sets)

    def _rewrite_filter(self, node: ccdag.Filter):
        """ Push down collusion sets for a Filter node. """

        out_rel_cols = node.out_rel.columns

        for in_col, out_col in zip(node.get_in_rel().columns, out_rel_cols):
            out_col.coll_sets |= copy.deepcopy(in_col.coll_sets)

    def _rewrite_multiply(self, node: ccdag.Multiply):
        """ Push down collusion sets for a Multiply node. """

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

    def _rewrite_hybrid_join(self, node: ccdag.HybridJoin):

        raise Exception("HybridJoin encountered during CollSetPropDown")

    def _rewrite_join(self, node: ccdag.Join):
        """ Push down collusion sets for a Join node. """

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

    def _rewrite_concat(self, node: ccdag.Concat):
        """ Push down collusion sets for a Concat node. """

        # Copy over columns from existing relation
        out_rel_cols = node.out_rel.columns

        # Combine per-column collusion sets
        for idx, col in enumerate(out_rel_cols):
            columns_at_idx = [in_rel.columns[idx] for in_rel in node.get_in_rels()]
            col.coll_sets = utils.coll_sets_from_columns(columns_at_idx)


class HybridOperatorOpt(DagRewriter):
    """ DagRewriter subclass specific to hybrid operator optimization rewriting. """

    def __init__(self):

        super(HybridOperatorOpt, self).__init__()

    def _rewrite_aggregate(self, node: ccdag.Aggregate):
        """ Convert Aggregate node to HybridAggregate node. """
        if node.is_mpc:
            out_rel = node.out_rel
            # TODO extend to multi-column case
            # by convention the group-by column comes first in the result of an aggregation
            group_col_idx = 0
            # oversimplifying here. what if there are multiple singleton coll_sets?
            singleton_coll_sets = filter(
                lambda s: len(s) == 1,
                out_rel.columns[group_col_idx].coll_sets)
            singleton_coll_sets = sorted(list(singleton_coll_sets))
            if singleton_coll_sets:
                trusted_party = next(iter(singleton_coll_sets[0]))
                hybrid_agg_op = ccdag.HybridAggregate.from_aggregate(node, trusted_party)
                parents = hybrid_agg_op.parents
                for par in parents:
                    par.replace_child(node, hybrid_agg_op)

    def _rewrite_reveal_join(self, node: ccdag.RevealJoin):
        # TODO
        pass

    def _rewrite_hybrid_join(self, node: ccdag.HybridJoin):

        raise Exception("HybridJoin encountered during HybridOperatorOpt")

    def _rewrite_hybrid_aggregate(self, node: ccdag.HybridAggregate):

        raise Exception("HybridAggregate encountered during HybridOperatorOpt")

    def _rewrite_join(self, node: ccdag.Join):
        """ Convert Join node to HybridJoin node. """
        if node.is_mpc:
            out_rel = node.out_rel
            # TODO this doesn't look right
            key_col_idx = 0
            # oversimplifying here. what if there are multiple singleton coll_sets?
            singleton_coll_sets = filter(
                lambda s: len(s) == 1,
                out_rel.columns[key_col_idx].coll_sets)
            singleton_coll_sets = sorted(list(singleton_coll_sets))
            if singleton_coll_sets:
                trusted_party = next(iter(singleton_coll_sets[0]))
                hybrid_join_op = ccdag.HybridJoin.from_join(node, trusted_party)
                parents = hybrid_join_op.parents
                for par in parents:
                    par.replace_child(node, hybrid_join_op)


# TODO: this class is messy
class InsertOpenAndCloseOps(DagRewriter):
    """
    Data structure for inserting Open and Close ops that separate
    MPC and non-MPC boundaries into the DAG.
    """

    def __init__(self):

        super(InsertOpenAndCloseOps, self).__init__()

    def _rewrite_default_unary(self, node: ccdag.UnaryOpNode):
        """
        Insert Store node beneath a UnaryOpNode that
        is at a lower boundary of an MPC op.
        """

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
                store_op = ccdag.Open(out_rel, None)
                store_op.is_mpc = True
                ccdag.insert_between_children(node, store_op)
            else:
                raise Exception(
                    "different stored_with on non-lower-boundary unary op", node)

    def _rewrite_hybrid_aggregate(self, node: ccdag.HybridAggregate):

        self._rewrite_default_unary(node)

    def _rewrite_aggregate(self, node: ccdag.Aggregate):

        self._rewrite_default_unary(node)

    def _rewrite_divide(self, node: ccdag.Divide):

        self._rewrite_default_unary(node)

    def _rewrite_distinct_count(self, node: ccdag.DistinctCount):

        self._rewrite_default_unary(node)

    def _rewrite_project(self, node: ccdag.Project):

        self._rewrite_default_unary(node)

    def _rewrite_filter(self, node: ccdag.Filter):

        self._rewrite_default_unary(node)

    def _rewrite_multiply(self, node: ccdag.Multiply):

        self._rewrite_default_unary(node)

    def _rewrite_hybrid_join(self, node: ccdag.HybridJoin):

        self._rewrite_join(node)

    def _rewrite_join(self, node: ccdag.Join):
        """
        Insert Open/Close ops above or beneath a Join node. If the parent of the Join node is an upper
        boundary node, then a Close op is inserted between it and the parent node. If the Join node is
        a leaf node (i.e. - has no children), then an Open op is inserted beneath it.
        """

        out_stored_with = node.out_rel.stored_with
        ordered_pars = [node.left_parent, node.right_parent]

        left_stored_with = node.get_left_in_rel().stored_with
        right_stored_with = node.get_right_in_rel().stored_with
        in_stored_with = left_stored_with | right_stored_with

        for parent in ordered_pars:
            if not parent.is_mpc and not isinstance(parent, ccdag.Close) and node.is_mpc:
                # Entering mpc mode so need to secret-share before op
                out_rel = copy.deepcopy(parent.out_rel)
                out_rel.rename(out_rel.name + "_close")
                out_rel.stored_with = copy.copy(in_stored_with)
                # create and insert close node
                close_op = ccdag.Close(out_rel, None)
                close_op.is_mpc = True
                ccdag.insert_between(parent, node, close_op)
        if node.is_leaf():
            if len(in_stored_with) > 1 and len(out_stored_with) == 1:
                target_party = next(iter(out_stored_with))
                node.out_rel.stored_with = copy.copy(in_stored_with)
                cc._open(node, node.out_rel.name + "_open", target_party)

    def _rewrite_concat(self, node: ccdag.Concat):
        """
        Insert a Close op above a Concat node if it's
        parent's stored_with sets do not match it's own.
        """
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
                store_op = ccdag.Close(out_rel, None)
                store_op.is_mpc = True
                ccdag.insert_between(parent, node, store_op)


class ExpandCompositeOps(DagRewriter):
    """
    Replaces operator nodes that correspond to composite operations
    (for example hybrid joins) into subdags of primitive operators.
    """

    def __init__(self, use_leaky_ops: bool = True):
        super(ExpandCompositeOps, self).__init__()
        self.use_leaky_ops = use_leaky_ops
        self.join_counter = 0
        self.agg_counter = 0

    def _create_unique_join_suffix(self):
        """
        Creates a unique string which will be appended to the end of each sub-relation created for each new hybrid
        join. This prevents relation name overlap in the case of multiple hybrid operators.
        """
        self.join_counter += 1
        return "_hybrid_join_" + str(self.join_counter)

    def _create_unique_agg_suffix(self):
        """
        Creates a unique string which will be appended to the end of each sub-relation created for each new hybrid
        aggregation. This prevents relation name overlap in the case of multiple hybrid operators.
        """
        self.agg_counter += 1
        return "_hybrid_agg_" + str(self.agg_counter)

    # This is obsolete now
    # def _rewrite_agg_leaky(self, node: ccdag.HybridAggregate):
    #     """
    #     Expand hybrid aggregation into a sub-dag of primitive operators. This uses the leaky version.
    #     """
    #     suffix = self._create_unique_agg_suffix()
    #     group_by_col_name = node.group_cols[0].name

    #     shuffled = cc.shuffle(node.parent, "shuffled" + suffix)
    #     shuffled.is_mpc = True
    #     node.parent.children.remove(node)

    #     persisted = cc._persist(shuffled, "persisted" + suffix)
    #     persisted.isMPC = True

    #     keys_closed = cc.project(shuffled, "keys_closed" + suffix, [group_by_col_name])
    #     keys_closed.isMPC = True

    #     keys_open = cc._open(keys_closed, "keys_open" + suffix, node.trusted_party)
    #     keys_open.isMPC = True

    #     indexed = cc.index(keys_open, "indexed" + suffix, "row_index")
    #     indexed.isMPC = False

    #     distinct_keys = cc.distinct(keys_open, "distinct_keys" + suffix, [group_by_col_name])
    #     distinct_keys.isMPC = False

    #     # TODO use persist
    #     persist_dist_keys = cc.project(distinct_keys, "persist_dist_keys" + suffix, [group_by_col_name])
    #     persist_dist_keys.isMPC = False

    #     indexed_distinct = cc.index(distinct_keys, "indexed_distinct" + suffix, "key_index")
    #     indexed_distinct.isMPC = False

    #     indexes_joined = cc.join(indexed, indexed_distinct, "indexes_joined" + suffix, [group_by_col_name],
    #                              [group_by_col_name])
    #     indexes_joined.isMPC = False

    #     # TODO: could project row indexes away too
    #     indexes_only = cc.project(indexes_joined, "indexes_only" + suffix, ["row_index", "key_index"])
    #     indexes_only.isMPC = False

    #     closed_distinct = cc._close(persist_dist_keys, "closed_distinct" + suffix, node.get_in_rel().stored_with)
    #     closed_distinct.isMPC = True
    #     keys_lookup = cc._close(indexes_only, "keys_lookup" + suffix, node.get_in_rel().stored_with)
    #     keys_lookup.isMPC = True

    #     group_col_names = [col.name for col in node.group_cols]
    #     out_over_col_name = node.out_rel.columns[-1].name

    #     result = cc._leaky_index_aggregate(persisted, node.out_rel.name, group_col_names, node.agg_col.name,
    #                                        node.aggregator,
    #                                        out_over_col_name,
    #                                        closed_distinct,
    #                                        keys_lookup)
    #     result.is_mpc = True
    #     # replace self with leaf of expanded subdag in each child node
    #     for child in node.get_sorted_children():
    #         child.replace_parent(node, result)
    #     # add former children to children of leaf
    #     result.children = node.children

    def _rewrite_agg_non_leaky(self, node: ccdag.HybridAggregate):
        """
        Expand hybrid aggregation into a sub-dag of primitive operators. This uses the size-leaking version.
        """
        suffix = self._create_unique_agg_suffix()
        group_by_col_name = node.group_cols[0].name

        shuffled = cc.shuffle(node.parent, "shuffled" + suffix)
        shuffled.is_mpc = True
        node.parent.children.remove(node)

        persisted = cc._persist(shuffled, "persisted" + suffix)
        persisted.is_mpc = True

        keys_closed = cc.project(shuffled, "keys_closed" + suffix, [group_by_col_name])
        keys_closed.is_mpc = True

        keys = cc._open(keys_closed, "keys" + suffix, node.trusted_party)
        keys.is_mpc = True

        indexed = cc.index(keys, "indexed" + suffix, "row_index")
        indexed.is_mpc = False

        sorted_by_key = cc.sort_by(indexed, "sorted_by_key" + suffix, group_by_col_name)
        sorted_by_key.is_mpc = False

        eq_flags = cc._comp_neighs(sorted_by_key, "eq_flags" + suffix, group_by_col_name)
        eq_flags.is_mpc = False

        # TODO check if we can use a persist here
        sorted_by_key_dummy = cc.project(sorted_by_key, "sorted_by_key_dummy" + suffix,
                                         ["row_index", group_by_col_name])
        sorted_by_key_dummy.is_mpc = False

        closed_eq_flags = cc._close(eq_flags, "closed_eq_flags" + suffix, node.get_in_rel().stored_with)
        closed_eq_flags.is_mpc = True

        closed_sorted_by_key = cc._close(sorted_by_key_dummy, "closed_sorted_by_key" + suffix,
                                         node.get_in_rel().stored_with)
        closed_sorted_by_key.is_mpc = True

        group_col_names = [col.name for col in node.group_cols]
        out_over_col_name = node.out_rel.columns[-1].name
        result = cc.index_aggregate(persisted, node.out_rel.name, group_col_names, node.agg_col.name, node.aggregator,
                                    out_over_col_name,
                                    closed_eq_flags,
                                    closed_sorted_by_key)
        result.is_mpc = True
        # replace self with leaf of expanded subdag in each child node
        for child in node.get_sorted_children():
            child.replace_parent(node, result)
        # add former children to children of leaf
        result.children = node.children

    def _rewrite_hybrid_aggregate(self, node: ccdag.HybridAggregate):
        # TODO cleaner way would be to have a LeakyHybridAggregate class
        if self.use_leaky_ops:
            raise Exception("not implemented")
            # self._rewrite_agg_leaky(node)
        else:
            self._rewrite_agg_non_leaky(node)

    # this is obsolete now
    # def _rewrite_join_leaky(self, node: ccdag.HybridJoin):
    #     """
    #     Expand hybrid join into a sub-dag of primitive operators. This uses the leaky version.
    #     """
    #     suffix = self._create_unique_join_suffix()
    #     # TODO column names should not be hard-coded

    #     # in left parents' children, replace self with first primitive operator
    #     # in expanded subdag
    #     shuffled_a = cc.shuffle(node.left_parent, "shuffled_a" + suffix)
    #     shuffled_a.is_mpc = True
    #     node.left_parent.children.remove(node)

    #     # same for right parent
    #     shuffled_b = cc.shuffle(node.right_parent, "shuffled_b" + suffix)
    #     shuffled_b.is_mpc = True
    #     node.right_parent.children.remove(node)

    #     persisted_b = cc._persist(shuffled_b, "persisted_b" + suffix)
    #     persisted_b.is_mpc = True
    #     persisted_a = cc._persist(shuffled_a, "persisted_a" + suffix)
    #     persisted_a.is_mpc = True

    #     keys_a_closed = cc.project(shuffled_a, "keys_a_closed" + suffix, ["a"])
    #     keys_a_closed.is_mpc = True
    #     keys_b_closed = cc.project(shuffled_b, "keys_b_closed" + suffix, ["c"])
    #     keys_b_closed.is_mpc = True

    #     keys_a = cc._open(keys_a_closed, "keys_a" + suffix, node.trusted_party)
    #     keys_a.is_mpc = True
    #     keys_b = cc._open(keys_b_closed, "keys_b" + suffix, node.trusted_party)
    #     keys_b.is_mpc = True

    #     indexed_a = cc.index(keys_a, "indexed_a" + suffix, "index_a")
    #     indexed_a.is_mpc = False

    #     indexed_b = cc.index(keys_b, "indexed_b" + suffix, "index_b")
    #     indexed_b.is_mpc = False

    #     joined_indices = cc.join(indexed_a, indexed_b, "joined_indices" + suffix, ["a"], ["c"])
    #     joined_indices.is_mpc = False

    #     indices_only = cc.project(
    #         joined_indices, "indices_only" + suffix, ["index_a", "index_b"])
    #     indices_only.is_mpc = False

    #     stored_with_union = node.get_left_in_rel().stored_with.union(node.get_right_in_rel().stored_with)
    #     indices_closed = cc._close(indices_only, "indices_closed" + suffix, stored_with_union)
    #     indices_closed.is_mpc = True

    #     joined = cc._index_join(persisted_a, persisted_b, node.out_rel.name, ["a"], ["c"], indices_closed)
    #     joined.is_mpc = True

    #     # replace self with leaf of expanded subdag in each child node
    #     for child in node.get_sorted_children():
    #         child.replace_parent(node, joined)
    #     # add former children to children of leaf
    #     joined.children = node.children

    def _rewrite_hybrid_join_non_leaky(self, node: ccdag.HybridJoin):
        """
        Expand hybrid join into a sub-dag of primitive operators. This uses the size-leaking version.
        """
        suffix = self._create_unique_join_suffix()
        # TODO column names should not be hard-coded

        # in left parents' children, replace self with first primitive operator
        # in expanded subdag
        left_shuffled = cc.shuffle(node.left_parent, "left_shuffled" + suffix)
        left_shuffled.is_mpc = True
        node.left_parent.children.remove(node)

        # same for right parent
        right_shuffled = cc.shuffle(node.right_parent, "right_shuffled" + suffix)
        right_shuffled.is_mpc = True
        node.right_parent.children.remove(node)

        left_persisted = cc._persist(left_shuffled, "left_persisted" + suffix)
        left_persisted.is_mpc = True

        right_persisted = cc._persist(right_shuffled, "right_persisted" + suffix)
        right_persisted.is_mpc = True

        left_keys_closed = cc.project(left_shuffled, "left_keys_closed" + suffix, ["a"])
        left_keys_closed.is_mpc = True

        right_keys_closed = cc.project(right_shuffled, "right_keys_closed" + suffix, ["c"])
        right_keys_closed.is_mpc = True

        left_keys_open = cc._open(left_keys_closed, "left_keys_open" + suffix, node.trusted_party)
        left_keys_open.is_mpc = True

        right_keys_open = cc._open(right_keys_closed, "right_keys_open" + suffix, node.trusted_party)
        right_keys_open.is_mpc = True

        # TODO remove dummy ops
        left_dummy = cc.project(left_keys_open, "left_dummy" + suffix, ["a"])
        left_dummy.is_mpc = False

        right_dummy = cc.project(right_keys_open, "right_dummy" + suffix, ["c"])
        right_dummy.is_mpc = False

        flags = cc._join_flags(left_dummy, right_dummy, "flags" + suffix, ["a"], ["c"])
        flags.is_mpc = False

        flags_closed = cc._close(flags, "flags_closed" + suffix, {1, 2, 3})
        flags_closed.is_mpc = True

        joined = cc._flag_join(left_persisted, right_persisted, node.out_rel.name, ["a"], ["c"], flags_closed)
        # replace self with leaf of expanded subdag in each child node
        for child in node.get_sorted_children():
            child.replace_parent(node, joined)
        # add former children to children of leaf
        joined.children = node.children

    def _rewrite_hybrid_join(self, node: ccdag.HybridJoin):
        if self.use_leaky_ops:
            raise Exception("not implemented")
        else:
            self._rewrite_hybrid_join_non_leaky(node)


class StoredWithSimplifier(DagRewriter):
    """
    Converts all stored_with sets larger than 1 to special all-parties-stored-with set.
    TODO this is a pre-deadline hack
    """

    def __init__(self, all_parties: set = None):
        super(StoredWithSimplifier, self).__init__()
        if all_parties is None:
            all_parties = {1, 2, 3}
        self.all_parties = all_parties

    def rewrite(self, dag: ccdag.OpDag):
        """ Traverse topologically sorted DAG, inspect each node. """
        ordered = dag.top_sort()
        if self.reverse:
            ordered = ordered[::-1]

        for node in ordered:
            print(type(self).__name__, "rewriting", node.out_rel.name)
            if len(node.out_rel.stored_with) > 1:
                node.out_rel.stored_with = self.all_parties


def rewrite_dag(dag: ccdag.OpDag, all_parties: list = None, use_leaky_ops: bool = False):
    """ Combines and calls all rewrite operations. """
    if all_parties is None:
        all_parties = [1, 2, 3]
    MPCPushDown().rewrite(dag)
    MPCPushUp().rewrite(dag)
    CollSetPropDown().rewrite(dag)
    HybridOperatorOpt().rewrite(dag)
    InsertOpenAndCloseOps().rewrite(dag)
    ExpandCompositeOps(use_leaky_ops).rewrite(dag)
    StoredWithSimplifier(set(all_parties)).rewrite(dag)
    return dag


def scotch(f: callable):
    """ Wraps protocol execution to only generate Scotch code. """

    from conclave.codegen import scotch
    from conclave import CodeGenConfig

    def wrap():
        code = scotch.ScotchCodeGen(CodeGenConfig(), f())._generate(None, None)
        return code

    return wrap


def mpc(*args):
    def _mpc(f):
        def wrapper():
            dag = rewrite_dag(ccdag.OpDag(f()))
            return dag

        return wrapper

    if len(args) == 1 and callable(args[0]):
        # No arguments, this is the decorator
        # Set default values for the arguments
        return _mpc(args[0])
    else:
        # This is just returning the decorator
        return _mpc


def dag_only(f: callable):
    """ Wrapper to bypass rewrite logic. """

    def wrap():
        return ccdag.OpDag(f())

    return wrap
