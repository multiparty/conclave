"""
Workflow graph optimizations and transformations.
"""
import copy
import warnings

import conclave.config as cc_conf
import conclave.dag as ccdag
import conclave.lang as cc
import conclave.utils as utils
from conclave.utils import defCol


def push_op_node_down(top_node: ccdag.OpNode, bottom_node: ccdag.OpNode):
    """
    Pushes a node that must be done under MPC further down in the DAG,
    and inserts its children (that can be carried out locally) above it.
    """

    # only dealing with one grandchild case for now
    assert (len(bottom_node.children) <= 1)
    child = next(iter(bottom_node.children), None)

    # remove bottom node between the bottom node's child and the top node
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


def split_agg(node: ccdag.Aggregate):
    """
    Splits an aggregation into two aggregations, one local, the other MPC.

    For now, deals with case where there is a Concat into an
    Aggregate. Here, local aggregation can be computed before
    concatenation, and then another aggregation under MPC.

    >>> cols_in = [
    ... defCol("a", "INTEGER", 1),
    ... defCol("b", "INTEGER", 1),
    ... defCol("c", "INTEGER", 1),
    ... defCol("d", "INTEGER", 1)]
    >>> in_op = cc.create("rel", cols_in, {1})
    >>> agged = cc.aggregate(in_op, "agged", ["d"], "c", "sum", "total")
    >>> split_agg(agged)
    >>> agged.out_rel.dbg_str()
    'agged([d {1}, total {1}]) {1}'
    >>> len(agged.children)
    1
    >>> child = next(iter(agged.children))
    >>> child.get_in_rel().dbg_str()
    'agged([d {1}, total {1}]) {1}'
    >>> child.group_cols[0].dbg_str()
    'd {1}'
    >>> child.group_cols[0].idx
    0
    >>> child.agg_col.idx
    1
    """

    # Only dealing with single child case for now
    assert (len(node.children) <= 1)
    clone = copy.deepcopy(node)

    assert clone.aggregator in {"sum", "count"}
    clone.aggregator = "sum"
    clone.out_rel.rename(node.out_rel.name + "_obl")

    assert (len(clone.group_cols) == 1)

    updated_group_col = copy.deepcopy(node.out_rel.columns[0])
    updated_group_col.idx = 0
    updated_over_col = copy.deepcopy(node.out_rel.columns[1])
    updated_over_col.idx = 1
    clone.group_cols = [updated_group_col]
    clone.agg_col = updated_over_col
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

    def __init__(self, conclave_config: cc_conf.CodeGenConfig):

        self.conclave_config = conclave_config
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
            elif isinstance(node, ccdag.PublicJoin):
                self._rewrite_public_join(node)
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
            elif isinstance(node, ccdag.PubJoin):
                self._rewrite_pub_join(node)
            elif isinstance(node, ccdag.ConcatCols):
                self._rewrite_concat_cols(node)
            elif isinstance(node, ccdag.SortBy):
                self._rewrite_sort_by(node)
            elif isinstance(node, ccdag.FilterBy):
                self._rewrite_filter_by(node)
            elif isinstance(node, ccdag.Union):
                self._rewrite_union(node)
            elif isinstance(node, ccdag.PubIntersect):
                self._rewrite_pub_intersect(node)
            elif isinstance(node, ccdag.Persist):
                self._rewrite_persist(node)
            elif isinstance(node, ccdag.IndexesToFlags):
                self._rewrite_indexes_to_flags(node)
            elif isinstance(node, ccdag.NumRows):
                self._rewrite_num_rows(node)
            elif isinstance(node, ccdag.Blackbox):
                self._rewrite_blackbox(node)
            elif isinstance(node, ccdag.Shuffle):
                self._rewrite_shuffle(node)
            elif isinstance(node, ccdag.Index):
                self._rewrite_index(node)
            elif isinstance(node, ccdag.CompNeighs):
                self._rewrite_comp_neighs(node)
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

    def _rewrite_public_join(self, node: ccdag.PublicJoin):
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

    def _rewrite_pub_join(self, node: ccdag.PubJoin):
        pass

    def _rewrite_concat_cols(self, node: ccdag.ConcatCols):
        pass

    def _rewrite_sort_by(self, node: ccdag.SortBy):
        pass

    def _rewrite_filter_by(self, node: ccdag.FilterBy):
        pass

    def _rewrite_union(self, node: ccdag.Union):
        pass

    def _rewrite_pub_intersect(self, node: ccdag.PubIntersect):
        pass

    def _rewrite_persist(self, node: ccdag.Persist):
        pass

    def _rewrite_indexes_to_flags(self, node: ccdag.IndexesToFlags):
        pass

    def _rewrite_num_rows(self, node: ccdag.NumRows):
        pass

    def _rewrite_blackbox(self, node: ccdag.Blackbox):
        pass

    def _rewrite_shuffle(self, node: ccdag.Shuffle):
        pass

    def _rewrite_index(self, node: ccdag.Index):
        pass

    def _rewrite_comp_neighs(self, node: ccdag.CompNeighs):
        pass


class MPCPushDown(DagRewriter):
    """ DagRewriter subclass for pushing MPC boundaries down in workflows. """

    def __init__(self, conclave_config: cc_conf.CodeGenConfig):
        """ Initialize MPCPushDown object. """

        super(MPCPushDown, self).__init__(conclave_config)

    @staticmethod
    def _do_commute(top_op: ccdag.OpNode, bottom_op: ccdag.OpNode):
        # TODO: over-simplified
        # TODO: add rules for other ops
        # TODO: agg (as we define it) doesn't commute with proj if proj re-arranges or drops columns

        if isinstance(top_op, ccdag.Aggregate):
            if isinstance(bottom_op, ccdag.Divide):
                return True
            elif top_op.aggregator == 'mean':
                return True
            elif top_op.aggregator == 'std_dev':
                return True
            else:
                return False
        else:
            return False

    @staticmethod
    def _rewrite_default(node: ccdag.OpNode):
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
                parent.update_out_rel_cols()
            elif isinstance(parent, ccdag.Aggregate) and self._do_commute(parent, node):
                agg_op = parent
                agg_parent = agg_op.parent
                if isinstance(agg_parent, ccdag.Concat) and agg_parent.is_boundary():
                    concat_op = agg_parent
                    assert len(concat_op.children) == 1
                    push_op_node_down(agg_op, node)
                    updated_node = agg_op.parent
                    push_op_node_down(concat_op, updated_node)
                    concat_op.update_out_rel_cols()
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
                if node.aggregator != "mean" and node.aggregator != "std_dev":
                    split_agg(node)
                    push_op_node_down(parent, node)
                    parent.update_out_rel_cols()
                else:
                    node.is_mpc = True
            else:
                node.is_mpc = True
        else:
            pass

    def _rewrite_project(self, node: ccdag.Project):

        self._rewrite_unary_default(node)

    def _rewrite_filter(self, node: ccdag.Filter):

        self._rewrite_default(node)

    def _rewrite_multiply(self, node: ccdag.Multiply):

        self._rewrite_unary_default(node)

    def _rewrite_divide(self, node: ccdag.Divide):

        self._rewrite_unary_default(node)

    def _rewrite_public_join(self, node: ccdag.PublicJoin):

        raise Exception("PublicJoin encountered during MPCPushDown")

    def _rewrite_hybrid_join(self, node: ccdag.HybridJoin):

        raise Exception("HybridJoin encountered during MPCPushDown")

    def _rewrite_join(self, node: ccdag.Join):

        self._rewrite_default(node)

    def _rewrite_concat_cols(self, node: ccdag.ConcatCols):

        self._rewrite_default(node)

    def _rewrite_concat(self, node: ccdag.Concat):
        """ Concat nodes with more than 1 child can be forked into multiple Concats. """

        if node.requires_mpc():
            node.is_mpc = True
            if len(node.children) > 1 and node.is_boundary():
                fork_node(node)

    def _rewrite_distinct_count(self, node: ccdag.DistinctCount):

        self._rewrite_default(node)

    def _rewrite_pub_join(self, node: ccdag.PubJoin):

        self._rewrite_default(node)

    def _rewrite_sort_by(self, node: ccdag.SortBy):

        self._rewrite_default(node)

    def _rewrite_filter_by(self, node: ccdag.FilterBy):

        self._rewrite_default(node)

    def _rewrite_union(self, node: ccdag.Union):

        self._rewrite_default(node)

    def _rewrite_pub_intersect(self, node: ccdag.PubIntersect):

        self._rewrite_default(node)

    def _rewrite_persist(self, node: ccdag.Persist):

        self._rewrite_default(node)

    def _rewrite_indexes_to_flags(self, node: ccdag.IndexesToFlags):

        self._rewrite_default(node)


class MPCPushUp(DagRewriter):
    """ DagRewriter subclass for pushing MPC boundary up in workflows. """

    def __init__(self, conclave_config: cc_conf.CodeGenConfig):
        """ Initialize MPCPushUp object. """

        super(MPCPushUp, self).__init__(conclave_config)
        self.reverse = True

    @staticmethod
    def _rewrite_unary_default(node: ccdag.UnaryOpNode):
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

    def _rewrite_public_join(self, node: ccdag.PublicJoin):

        raise Exception("PublicJoin encountered during MPCPushUp")

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

    def _rewrite_concat_cols(self, node: ccdag.ConcatCols):
        # TODO hack hack hack
        node.is_mpc = True

    def _rewrite_create(self, node: ccdag.Create):

        pass

    def _rewrite_pub_join(self, node: ccdag.PubJoin):

        pass


class UpdateColumns(DagRewriter):
    """
    Updates all operator specific columns after the pushdown pass.
    """

    def __init__(self, conclave_config: cc_conf.CodeGenConfig):
        super(UpdateColumns, self).__init__(conclave_config)

    def rewrite(self, dag: ccdag.OpDag):
        ordered = dag.top_sort()
        for node in ordered:
            print(type(self).__name__, "rewriting", node.out_rel.name)
            node.update_op_specific_cols()


class TrustSetPropDown(DagRewriter):
    """
    Propagates trust sets down through the DAG.

    Trust sets are column-specific and thus more granular than stored_with sets, which are defined over whole relations.
    """

    def __init__(self, conclave_config: cc_conf.CodeGenConfig):

        super(TrustSetPropDown, self).__init__(conclave_config)

    @staticmethod
    def _rewrite_linear_op(node: [ccdag.Divide, ccdag.Multiply]):

        out_rel_cols = node.out_rel.columns
        operands = node.operands
        target_col = node.target_col

        # Update target column collusion set
        target_col_out = out_rel_cols[target_col.idx]

        # Need all operands to derive target column
        target_col_out.trust_set = copy.copy(utils.trust_set_from_columns(operands))

        # The other columns weren't modified so their trust sets simply carry over
        # Skip target column (which comes first)
        zipped = zip(node.get_in_rel().columns[1:], out_rel_cols[1:])
        for in_col, out_col in zipped:
            out_col.trust_set = copy.copy(in_col.trust_set)

    def _rewrite_aggregate(self, node: [ccdag.Aggregate, ccdag.IndexAggregate]):
        """
        Push down trust sets for an Aggregate or IndexAggregate node.

        >>> cols_in = [defCol("a", "INTEGER", 1, 2), defCol("b", "INTEGER", 1)]
        >>> in_op = cc.create("rel", cols_in, {1})
        >>> agged = cc.aggregate(in_op, "agged", ["a"], "b", "sum", "total_b")
        >>> TrustSetPropDown()._rewrite_aggregate(agged)
        >>> agged.out_rel.columns[0].dbg_str()
        'a {1 2}'
        >>> agged.out_rel.columns[1].dbg_str()
        'total_b {1}'
        """
        in_group_cols = node.group_cols
        out_group_cols = node.out_rel.columns[:-1]
        in_group_cols_ts = utils.trust_set_from_columns(in_group_cols)
        for i in range(len(out_group_cols)):
            out_group_cols[i].trust_set = copy.copy(in_group_cols_ts)
        if node.aggregator == "sum" or node.aggregator == "mean" or node.aggregator == "std_dev":
            in_agg_col_ts = copy.copy(node.agg_col.trust_set)
        elif node.aggregator == "count":
            # in case of a count, result over col has same trust set as group by cols
            in_agg_col_ts = in_group_cols_ts
        else:
            raise Exception("Unknown aggregator {}".format(node.aggregator))
        out_agg_col = node.out_rel.columns[-1]
        out_agg_col.trust_set = copy.copy(utils.merge_coll_sets(in_agg_col_ts, in_group_cols_ts))

    def _rewrite_divide(self, node: ccdag.Divide):
        """
        Push down trust sets for a Divide node.

        >>> cols_in = [defCol("a", "INTEGER", 1, 2), defCol("b", "INTEGER", 1, 3)]
        >>> in_op = cc.create("rel", cols_in, {1})
        >>> div = cc.divide(in_op, "div", "a", ["a", "b"])
        >>> TrustSetPropDown()._rewrite_divide(div)
        >>> div.out_rel.columns[0].dbg_str()
        'a {1}'
        >>> div.out_rel.columns[1].dbg_str()
        'b {1 3}'
        """
        TrustSetPropDown._rewrite_linear_op(node)

    def _rewrite_project(self, node: ccdag.Project):
        """
        Push down trust sets for a Project node.

        >>> cols_in = [defCol("a", "INTEGER", 1, 2), defCol("b", "INTEGER", 3)]
        >>> in_op = cc.create("rel", cols_in, {1})
        >>> proj = cc.project(in_op, "proj", ["b", "a"])
        >>> TrustSetPropDown()._rewrite_project(proj)
        >>> proj.out_rel.columns[0].dbg_str()
        'b {3}'
        >>> proj.out_rel.columns[1].dbg_str()
        'a {1 2}'
        """

        selected_cols = node.selected_cols

        for in_col, out_col in zip(selected_cols, node.out_rel.columns):
            out_col.trust_set = copy.copy(in_col.trust_set)

    def _rewrite_filter(self, node: ccdag.Filter):
        """
        Push down trust sets for a Filter node.

        >>> cols_in = [defCol("a", "INTEGER", 1, 2), defCol("b", "INTEGER", 1), defCol("c", "INTEGER", 3)]
        >>> in_op = cc.create("rel", cols_in, {1})
        >>> filt = cc.cc_filter(in_op, "filt", "a", "==", "b")
        >>> TrustSetPropDown()._rewrite_filter(filt)
        >>> filt.out_rel.columns[0].dbg_str()
        'a {1}'
        >>> filt.out_rel.columns[1].dbg_str()
        'b {1}'
        >>> filt.out_rel.columns[2].dbg_str()
        'c {}'
        """

        # To determine the result of a filter, we need to know all columns used in the filter condition
        condition_trust_set = utils.trust_set_from_columns([node.filter_col, node.other_col])

        out_rel_cols = node.out_rel.columns
        for in_col, out_col in zip(node.get_in_rel().columns, out_rel_cols):
            out_col.trust_set = utils.merge_coll_sets(condition_trust_set, in_col.trust_set)

    def _rewrite_filter_by(self, node: ccdag.FilterBy):
        """
        Push down trust sets for a FilterBy node.

        >>> cols_in = [defCol("a", "INTEGER", 1, 2, 3), defCol("b", "INTEGER", 1), defCol("c", "INTEGER", 3)]
        >>> in_op = cc.create("rel", cols_in, {1})
        >>> cols_in_keys = [defCol("k", "INTEGER", 1, 2, 3)]
        >>> keys_op = cc.create("keys", cols_in_keys, {1})
        >>> filt = cc.filter_by(in_op, "filt", "a", keys_op)
        >>> TrustSetPropDown()._rewrite_filter_by(filt)
        >>> filt.out_rel.columns[0].dbg_str()
        'a {1 2 3}'
        >>> filt.out_rel.columns[1].dbg_str()
        'b {1}'
        >>> filt.out_rel.columns[2].dbg_str()
        'c {3}'
        """

        # To determine the result of a filter by, we need to know all columns used in the filter by condition
        condition_trust_set = utils.trust_set_from_columns([node.filter_col, node.right_parent.out_rel.columns[0]])
        out_rel_cols = node.out_rel.columns
        for in_col, out_col in zip(node.get_left_in_rel().columns, out_rel_cols):
            out_col.trust_set = utils.merge_coll_sets(condition_trust_set, in_col.trust_set)

    def _rewrite_multiply(self, node: ccdag.Multiply):
        """
        Push down trust sets for a Multiply node.

        >>> cols_in = [defCol("a", "INTEGER", 1, 2), defCol("b", "INTEGER", 1, 3)]
        >>> in_op = cc.create("rel", cols_in, {1})
        >>> div = cc.multiply(in_op, "div", "a", ["a", "b"])
        >>> TrustSetPropDown()._rewrite_multiply(div)
        >>> div.out_rel.columns[0].dbg_str()
        'a {1}'
        >>> div.out_rel.columns[1].dbg_str()
        'b {1 3}'
        """
        TrustSetPropDown._rewrite_linear_op(node)

    def _rewrite_hybrid_join(self, node: ccdag.HybridJoin):

        raise Exception("HybridJoin encountered during CollSetPropDown")

    def _rewrite_join(self, node: ccdag.Join):
        """
        Push down trust sets for a Join node.

        >>> cols_in_left = [defCol("a", "INTEGER", 1), defCol("b", "INTEGER", 1)]
        >>> cols_in_right = [defCol("c", "INTEGER", 1, 2), defCol("d", "INTEGER", 2)]
        >>> left = cc.create("left", cols_in_left, {1})
        >>> right = cc.create("right", cols_in_right, {2})
        >>> joined = cc.join(left, right, "joined", ["a"], ["c"])
        >>> TrustSetPropDown()._rewrite_join(joined)
        >>> joined.out_rel.columns[0].dbg_str()
        'a {1}'
        >>> joined.out_rel.columns[1].dbg_str()
        'b {1}'
        >>> joined.out_rel.columns[2].dbg_str()
        'd {}'
        """

        left_in_rel = node.get_left_in_rel()
        right_in_rel = node.get_right_in_rel()

        left_join_cols = node.left_join_cols
        right_join_cols = node.right_join_cols

        num_join_cols = len(left_join_cols)

        out_join_cols = node.out_rel.columns[:num_join_cols]
        key_cols_coll_sets = []
        for i in range(len(left_join_cols)):
            key_cols_coll_sets.append(utils.merge_coll_sets(
                left_join_cols[i].trust_set, right_join_cols[i].trust_set))
            out_join_cols[i].trust_set = key_cols_coll_sets[i]

        abs_idx = len(left_join_cols)
        for in_col in left_in_rel.columns:
            if in_col not in set(left_join_cols):
                for key_col_coll_sets in key_cols_coll_sets:
                    node.out_rel.columns[abs_idx].trust_set = \
                        utils.merge_coll_sets(key_col_coll_sets, in_col.trust_set)
                abs_idx += 1

        for in_col in right_in_rel.columns:
            if in_col not in set(right_join_cols):
                for key_col_coll_sets in key_cols_coll_sets:
                    node.out_rel.columns[abs_idx].trust_set = \
                        utils.merge_coll_sets(key_col_coll_sets, in_col.trust_set)
                abs_idx += 1

    def _rewrite_union(self, node: ccdag.Union):
        """
        Push down trust sets for a Union node.

        >>> cols_in_left = [defCol("a", "INTEGER", 1, 2, 3), defCol("b", "INTEGER", 1)]
        >>> cols_in_right = [defCol("c", "INTEGER", 1, 2, 3), defCol("d", "INTEGER", 2)]
        >>> left = cc.create("left", cols_in_left, {1})
        >>> right = cc.create("right", cols_in_right, {2})
        >>> unioned = cc.union(left, right, "unioned", "a", "c")
        >>> TrustSetPropDown()._rewrite_union(unioned)
        >>> unioned.out_rel.columns[0].dbg_str()
        'a {1 2 3}'
        """
        left_col = node.left_col
        right_col = node.right_col
        node.out_rel.columns[0].trust_set = utils.trust_set_from_columns([left_col, right_col])

    def _rewrite_pub_intersect(self, node: ccdag.PubIntersect):
        """
        Push down trust sets for a PubIntersect node.

        >>> cols_in_left = [defCol("a", "INTEGER", 1, 2, 3), defCol("b", "INTEGER", 1)]
        >>> left = cc.create("left", cols_in_left, {1})
        >>> intersected = cc._pub_intersect(left, "intersected", "a")
        >>> TrustSetPropDown()._rewrite_pub_intersect(intersected)
        >>> intersected.out_rel.columns[0].dbg_str()
        'a {1 2 3}'
        """
        # TODO make more robust by wrapping two sides of intersect into single operator
        # This works because we can only run a PubIntersect on a public col
        node.out_rel.columns[0].trust_set = copy.deepcopy(node.col.trust_set)

    def _rewrite_concat(self, node: ccdag.Concat):
        """
        Push down trust sets for a Concat node.

        >>> cols_in_left = [defCol("a", "INTEGER", 1, 2), defCol("b", "INTEGER", 2)]
        >>> cols_in_right = [defCol("c", "INTEGER", 1, 3), defCol("b", "INTEGER", 3)]
        >>> left = cc.create("left", cols_in_left, {2})
        >>> right = cc.create("right", cols_in_right, {3})
        >>> rel = cc.concat([left, right], "rel")
        >>> TrustSetPropDown()._rewrite_concat(rel)
        >>> rel.out_rel.columns[0].dbg_str()
        'a {1}'
        >>> rel.out_rel.columns[1].dbg_str()
        'b {}'
        """

        # Copy over columns from existing relation
        out_rel_cols = node.out_rel.columns

        # Combine per-column collusion sets
        for idx, col in enumerate(out_rel_cols):
            columns_at_idx = [in_rel.columns[idx] for in_rel in node.get_in_rels()]
            col.trust_set = utils.trust_set_from_columns(columns_at_idx)


class HybridOperatorOpt(DagRewriter):
    """ DagRewriter subclass specific to hybrid operator optimization rewriting. """

    def __init__(self, conclave_config: cc_conf.CodeGenConfig):

        super(HybridOperatorOpt, self).__init__(conclave_config)

    def _rewrite_aggregate(self, node: ccdag.Aggregate):
        """ Convert Aggregate node to HybridAggregate node. """
        if node.is_mpc:
            out_rel = node.out_rel
            # TODO extend to multi-column case
            # by convention the group-by column comes first in the result of an aggregation
            group_col_idx = 0
            trust_set = out_rel.columns[group_col_idx].trust_set
            if trust_set:
                # hybrid agg possible
                # oversimplifying here. what if there are multiple STPs?
                STP = sorted(trust_set)[0]
                hybrid_agg_op = ccdag.HybridAggregate.from_aggregate(node, STP)
                parents = hybrid_agg_op.parents
                for par in parents:
                    par.replace_child(node, hybrid_agg_op)

    def _rewrite_public_join(self, node: ccdag.PublicJoin):

        raise Exception("PublicJoin encountered during HybridOperatorOpt")

    def _rewrite_hybrid_join(self, node: ccdag.HybridJoin):

        raise Exception("HybridJoin encountered during HybridOperatorOpt")

    def _rewrite_hybrid_aggregate(self, node: ccdag.HybridAggregate):

        raise Exception("HybridAggregate encountered during HybridOperatorOpt")

    def _rewrite_join(self, node: ccdag.Join):
        """ Convert Join node to HybridJoin node. """
        if node.is_mpc:
            out_rel = node.out_rel
            # TODO extend to multi-column case
            # by convention the join key columns come first in the result of a join
            key_col_idx = 0
            trust_set = out_rel.columns[key_col_idx].trust_set
            if trust_set:
                # for now only support 2-party public join
                in_stored_with = node.get_left_in_rel().stored_with | node.get_right_in_rel().stored_with
                if trust_set == set(self.conclave_config.all_pids) and len(in_stored_with) == 2:
                    # public join possible
                    public_join_op = ccdag.PublicJoin.from_join(node)
                    parents = public_join_op.parents
                    for par in parents:
                        par.replace_child(node, public_join_op)
                        # the concat will happen after the public join, under MPC
                        if isinstance(par, ccdag.Concat):
                            par.is_mpc = False
                            par.skip = True
                else:
                    # hybrid join possible
                    # oversimplifying here. what if there are multiple STPs?
                    STP = sorted(trust_set)[0]
                    hybrid_join_op = ccdag.HybridJoin.from_join(node, STP)
                    parents = hybrid_join_op.parents
                    for par in parents:
                        par.replace_child(node, hybrid_join_op)


# TODO: this class is messy
class InsertOpenAndCloseOps(DagRewriter):
    """
    Data structure for inserting Open and Close ops that separate
    MPC and non-MPC boundaries into the DAG.
    """

    def __init__(self, conclave_config: cc_conf.CodeGenConfig):

        super(InsertOpenAndCloseOps, self).__init__(conclave_config)

    @staticmethod
    def _rewrite_default_unary(node: ccdag.UnaryOpNode):
        """
        Insert Store node beneath a UnaryOpNode that
        is at a lower boundary of an MPC op.
        """
        # TODO: can there be a case when children have different stored_with sets?
        warnings.warn("hacky insert store ops")
        in_stored_with = node.get_in_rel().stored_with
        out_stored_with = node.out_rel.stored_with
        print(node.is_mpc)
        print(node.children)
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

    def _rewrite_public_join(self, node: ccdag.PublicJoin):

        self._rewrite_join(node)

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
        Insert a Close op above a Concat node if its parents' stored_with sets do not match its own.
        """
        if not node.skip:
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

    def _rewrite_concat_cols(self, node: ccdag.ConcatCols):
        # TODO this is a mess...
        out_stored_with = node.out_rel.stored_with
        ordered_pars = node.get_sorted_parents()
        in_stored_with = set()
        for parent in ordered_pars:
            par_stored_with = parent.out_rel.stored_with
            in_stored_with |= par_stored_with
        for parent in ordered_pars:
            if not isinstance(parent, ccdag.Close):
                out_rel = copy.deepcopy(parent.out_rel)
                out_rel.rename(out_rel.name + "_close")
                out_rel.stored_with = copy.copy(in_stored_with)
                # create and insert close node
                store_op = ccdag.Close(out_rel, None)
                store_op.is_mpc = True
                ccdag.insert_between(parent, node, store_op)

        if node.is_leaf():
            if len(in_stored_with) > 1 and len(out_stored_with) == 1:
                target_party = next(iter(out_stored_with))
                node.out_rel.stored_with = copy.copy(in_stored_with)
                cc._open(node, node.out_rel.name + "_open", target_party)

    def _rewrite_sort_by(self, node: ccdag.SortBy):
        self._rewrite_default_unary(node)


class ExpandCompositeOps(DagRewriter):
    """
    Replaces operator nodes that correspond to composite operations
    (for example hybrid joins) into subdags of primitive operators.
    """

    def __init__(self, conclave_config: cc_conf.CodeGenConfig):
        super(ExpandCompositeOps, self).__init__(conclave_config)

        self.use_leaky_ops = conclave_config.use_leaky_ops
        self.join_counter = 0
        self.public_join_counter = 0
        self.agg_counter = 0
        self.public_agg_counter = 0

    def _create_unique_public_join_suffix(self):
        """
        Creates a unique string which will be appended to the end of each sub-relation created for each new public
        join. This prevents relation name overlap in the case of multiple hybrid operators.
        """
        self.public_join_counter += 1
        return "_public_join_" + str(self.public_join_counter)

    def _create_unique_join_suffix(self):
        """
        Creates a unique string which will be appended to the end of each sub-relation created for each new hybrid
        join. This prevents relation name overlap in the case of multiple hybrid operators.
        """
        self.join_counter += 1
        return "_hybrid_join_" + str(self.join_counter)

    def _create_unique_public_agg_suffix(self):
        """
        Creates a unique string which will be appended to the end of each sub-relation created for each new public
        aggregation. This prevents relation name overlap in the case of multiple hybrid operators.
        """
        self.public_agg_counter += 1
        return "_public_agg_" + str(self.public_agg_counter)

    def _create_unique_agg_suffix(self):
        """
        Creates a unique string which will be appended to the end of each sub-relation created for each new hybrid
        aggregation. This prevents relation name overlap in the case of multiple hybrid operators.
        """
        self.agg_counter += 1
        return "_hybrid_agg_" + str(self.agg_counter)

    def _rewrite_agg_leaky(self, node: ccdag.HybridAggregate):
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
            self._rewrite_agg_leaky(node)
        else:
            raise Exception("not implemented")

    def _rewrite_hybrid_join_leaky(self, node: ccdag.HybridJoin):
        """
        Expand hybrid join into a sub-dag of primitive operators. This uses the size-leaking version.
        """
        suffix = self._create_unique_join_suffix()
        # TODO column names should not be hard-coded

        # Under MPC
        # in left parents' children, replace self with first primitive operator in expanded sub-dag
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

        # At STP
        left_indexed = cc.index(left_keys_open, "left_indexed" + suffix, "lidx")
        left_indexed.is_mpc = False
        # left_indexed_pers = cc._persist(left_indexed, "left_indexed" + suffix)

        right_indexed = cc.index(right_keys_open, "right_indexed" + suffix, "ridx")
        right_indexed.is_mpc = False
        # right_indexed_pers = cc._persist(right_indexed, "right_indexed" + suffix)

        joined_indexes = cc.join(left_indexed, right_indexed, "joined_indexes" + suffix, ["a"], ["c"])
        joined_indexes.is_mpc = False

        indexes_left = cc.project(joined_indexes, "indexes_left" + suffix, ["lidx"])
        indexes_left_pers = cc._persist(indexes_left, "indexes_left" + suffix)
        num_lookups = cc.num_rows(indexes_left, "num_lookups" + suffix)

        left_unpermuted = cc._unpermute(indexes_left, "left_unpermuted" + suffix, "lidx", "ll")
        left_unpermuted_idx = cc.project(left_unpermuted, "left_unpermuted_idx" + suffix, ["ll"])

        indexes_right = cc.project(joined_indexes, "indexes_right" + suffix, ["ridx"])
        indexes_right_pers = cc._persist(indexes_right, "indexes_right" + suffix)
        right_unpermuted = cc._unpermute(indexes_right, "right_unpermuted" + suffix, "ridx", "rr")
        right_unpermuted_idx = cc.project(right_unpermuted, "right_unpermuted_idx" + suffix, ["rr"])

        left_unpermuted_proj = cc.project(left_unpermuted, "left_unpermuted_proj" + suffix, ["lidx"])
        left_encoded = cc._indexes_to_flags(left_indexed, left_unpermuted_proj,
                                            "left_encoded" + suffix)
        left_encoded.is_mpc = False

        right_unpermuted_proj = cc.project(right_unpermuted, "right_unpermuted_proj" + suffix, ["ridx"])
        right_encoded = cc._indexes_to_flags(right_indexed, right_unpermuted_proj,
                                             "right_encoded" + suffix)
        right_encoded.is_mpc = False

        num_lookups_closed = cc._close(num_lookups, "num_lookups_closed" + suffix, set(self.conclave_config.all_pids))
        num_lookups_closed.is_mpc = True

        left_unpermuted_closed = \
            cc._close(left_unpermuted_idx, "left_unpermuted_closed" + suffix, set(self.conclave_config.all_pids))
        left_unpermuted_closed.is_mpc = True
        left_unpermuted_pers = cc._persist(left_unpermuted_closed, "left_unpermuted_closed" + suffix)
        right_unpermuted_closed = \
            cc._close(right_unpermuted_idx, "right_unpermuted_closed" + suffix, set(self.conclave_config.all_pids))
        right_unpermuted_closed.is_mpc = True
        right_unpermuted_pers = cc._persist(right_unpermuted_closed, "right_unpermuted_closed" + suffix)

        left_encoded_closed = \
            cc._close(left_encoded, "left_encoded_closed" + suffix, set(self.conclave_config.all_pids))
        left_encoded_closed.is_mpc = True
        right_encoded_closed = \
            cc._close(right_encoded, "right_encoded_closed" + suffix, set(self.conclave_config.all_pids))
        right_encoded_closed.is_mpc = True

        # TODO update operator name and arguments
        left_flags_and_indexes_closed = cc._flag_join(left_persisted, left_encoded_closed,
                                                      "left_flags_and_indexes_closed" + suffix,
                                                      ["a"], ["c"],
                                                      num_lookups_closed)
        left_flags_and_indexes_closed.is_mpc = True

        left_flags_and_indexes = cc._open(left_flags_and_indexes_closed, "left_flags_and_indexes" + suffix,
                                          node.trusted_party)
        left_flags_and_indexes.is_mpc = True

        right_flags_and_indexes_closed = cc._flag_join(right_persisted, right_encoded_closed,
                                                       "right_flags_and_indexes_closed" + suffix,
                                                       ["a"], ["c"],
                                                       num_lookups_closed)
        right_flags_and_indexes_closed.is_mpc = True

        right_flags_and_indexes = cc._open(right_flags_and_indexes_closed, "right_flags_and_indexes" + suffix,
                                           node.trusted_party)
        right_flags_and_indexes.is_mpc = True

        # Back at STP
        left_arranged = cc._indexes_to_flags(indexes_left_pers, left_flags_and_indexes, "left_arranged" + suffix,
                                             stage=1)
        left_arranged.is_mpc = False

        right_arranged = cc._indexes_to_flags(indexes_right_pers, right_flags_and_indexes, "right_arranged" + suffix,
                                              stage=1)
        right_arranged.is_mpc = False

        left_arranged_closed = \
            cc._close(left_arranged, "left_arranged_closed" + suffix, set(self.conclave_config.all_pids))
        left_arranged_closed.is_mpc = True

        right_arranged_closed = \
            cc._close(right_arranged, "right_arranged_closed" + suffix, set(self.conclave_config.all_pids))
        right_arranged_closed.is_mpc = True

        # Final MPC step
        left_inst = "    pd_shared3p uint32 [[2]] {} = oblIdxStepTwo(\"{}\", {}, {});\n".format(
            "left_res" + suffix,
            left_flags_and_indexes_closed.out_rel.name,
            left_arranged_closed.out_rel.name,
            left_unpermuted_pers.out_rel.name
        )
        left_res = cc.blackbox([left_arranged_closed, left_unpermuted_pers], "left_res" + suffix, ["a", "b"],
                               "sharemind",
                               left_inst)
        left_res.is_mpc = True

        right_inst = "    pd_shared3p uint32 [[2]] {} = oblIdxStepTwo(\"{}\", {}, {});\n".format(
            "right_res" + suffix,
            right_flags_and_indexes_closed.out_rel.name,
            right_arranged_closed.out_rel.name,
            right_unpermuted_pers.out_rel.name
        )
        right_res = cc.blackbox([right_arranged_closed, right_unpermuted_pers], "right_res" + suffix, ["c", "d"],
                                "sharemind",
                                right_inst)
        right_res.is_mpc = True

        comb_inst = "   pd_shared3p uint32 [[2]] {} = combineJoinSides({}, {});\n".format(
            node.out_rel.name,
            left_res.out_rel.name,
            right_res.out_rel.name
        )
        joined = cc.blackbox([left_res, right_res], node.out_rel.name,
                             [col.name for col in node.out_rel.columns], "sharemind", comb_inst)
        joined.is_mpc = True

        # replace self with leaf of expanded subdag in each child node
        for child in node.get_sorted_children():
            child.replace_parent(node, joined)
        # add former children to children of leaf
        joined.children = node.children

    def _rewrite_hybrid_join(self, node: ccdag.HybridJoin):
        if self.use_leaky_ops:
            self._rewrite_hybrid_join_leaky(node)
        else:
            raise Exception("not implemented")

    def _rewrite_public_join(self, node: ccdag.PublicJoin):

        suffix = self._create_unique_public_join_suffix()

        left_parent = node.left_parent
        right_parent = node.right_parent

        if isinstance(left_parent, ccdag.Concat):
            left_one = next(op for op in left_parent.parents if op.out_rel.stored_with == {1})
            left_two = next(op for op in left_parent.parents if op.out_rel.stored_with == {2})
            left_one.children.remove(left_parent)
            left_two.children.remove(left_parent)
            left_parent.skip = True
        else:
            raise Exception("Not supported for now")

        if isinstance(right_parent, ccdag.Concat):
            right_one = next(op for op in right_parent.parents if op.out_rel.stored_with == {1})
            right_two = next(op for op in right_parent.parents if op.out_rel.stored_with == {2})
            right_one.children.remove(right_parent)
            right_two.children.remove(right_parent)
            right_parent.skip = True
        else:
            raise Exception("Not supported for now")

        op_host = self.conclave_config.network_config["parties"][1]["host"]
        op_port = self.conclave_config.network_config["parties"][1]["port"]

        left_join = \
            cc._pub_join(left_one, "left_join" + suffix, node.left_join_cols[0].name,
                         other_op_node=right_one, host=op_host, port=op_port)
        right_join = cc._pub_join(left_two, "right_join" + suffix, node.left_join_cols[0].name, is_server=False,
                                  other_op_node=right_two, host=op_host, port=op_port)

        node.left_parent.children.remove(node)
        node.right_parent.children.remove(node)

        left_join_closed = cc._close(left_join, "left_join_closed" + suffix, set(self.conclave_config.all_pids))
        left_join_closed.is_mpc = True
        right_join_closed = cc._close(right_join, "right_join_closed" + suffix, set(self.conclave_config.all_pids))
        right_join_closed.is_mpc = True

        joined = cc.concat_cols([left_join_closed, right_join_closed], node.out_rel.name, use_mult=True)
        joined.is_mpc = True
        # replace self with leaf of expanded subdag in each child node
        for child in node.get_sorted_children():
            child.replace_parent(node, joined)
        # add former children to children of leaf
        joined.children = node.children


class StoredWithSimplifier(DagRewriter):
    """
    Converts all stored_with sets larger than 1 to special all-parties-stored-with set.
    TODO this is a pre-deadline hack
    """

    def __init__(self, conclave_config: cc_conf.CodeGenConfig):
        super(StoredWithSimplifier, self).__init__(conclave_config)

    def rewrite(self, dag: ccdag.OpDag):
        """ Traverse topologically sorted DAG, inspect each node. """
        ordered = dag.top_sort()
        if self.reverse:
            ordered = ordered[::-1]

        for node in ordered:
            print(type(self).__name__, "rewriting", node.out_rel.name)
            if len(node.out_rel.stored_with) > 1:
                node.out_rel.stored_with = set(self.conclave_config.all_pids)


class EliminateSorts(DagRewriter):
    """
    Eliminates redundant sorts when possible by tracking sorted columns throughout dag.
    """

    def __init__(self, conclave_config: cc_conf.CodeGenConfig):
        super(EliminateSorts, self).__init__(conclave_config)
        self.sorted_by = None

    def _rewrite_pub_join(self, node: ccdag.PubJoin):
        # first col is key col
        self.sorted_by = node.out_rel.columns[0]

    def _rewrite_project(self, node: ccdag.Project):
        # TODO only here for now to prevent dropped and re-ordered columns from breaking the propagation
        self.sorted_by = None

    def _order_preserving(self, node: ccdag.OpNode):
        if self.sorted_by:
            self.sorted_by = utils.find(node.out_rel.columns, self.sorted_by.name)

    def _non_order_preserving(self):
        self.sorted_by = None

    def _rewrite_concat_cols(self, node: ccdag.ConcatCols):
        self._order_preserving(node)

    def _rewrite_filter(self, node: ccdag.Filter):
        self._order_preserving(node)

    def _rewrite_blackbox(self, node: ccdag.Blackbox):
        self._non_order_preserving()

    def _rewrite_sort_by(self, node: ccdag.SortBy):
        self.sorted_by = utils.find(node.out_rel.columns, node.sort_by_col.name)

    def _rewrite_shuffle(self, node: ccdag.Shuffle):
        self.sorted_by = None

    def _rewrite_distinct_count(self, node: ccdag.DistinctCount):
        if node.is_mpc:
            if node.selected_col == self.sorted_by:
                node.use_sort = False
            self.sorted_by = node.out_rel.columns[0]
        else:
            self.sorted_by = None


def rewrite_dag(dag: ccdag.OpDag, conclave_config: cc_conf.CodeGenConfig):
    """ Combines and calls all rewrite operations. """
    MPCPushDown(conclave_config).rewrite(dag)
    UpdateColumns(conclave_config).rewrite(dag)
    MPCPushUp(conclave_config).rewrite(dag)
    TrustSetPropDown(conclave_config).rewrite(dag)
    HybridOperatorOpt(conclave_config).rewrite(dag)
    InsertOpenAndCloseOps(conclave_config).rewrite(dag)
    ExpandCompositeOps(conclave_config).rewrite(dag)
    StoredWithSimplifier(conclave_config).rewrite(dag)
    EliminateSorts(conclave_config).rewrite(dag)
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
