"""
Embedded language for relational workflows.
"""
import copy

import conclave.dag as cc_dag
import conclave.utils as utils
from conclave import rel
from conclave.rel import Column


def create(rel_name: str, columns: list, stored_with: set):
    """
    Define Create operation.

    :param rel_name: Name of returned Create node.
    :param columns: List of column objects.
    :param stored_with: Set of input party IDs that own this relation.
    :return: Create OpNode.
    """

    columns = [rel.Column(rel_name, col_name, idx, type_str, collusion_set)
               for idx, (col_name, type_str, collusion_set) in enumerate(columns)]
    out_rel = rel.Relation(rel_name, columns, stored_with)
    op = cc_dag.Create(out_rel)
    return op


def aggregate(input_op_node: cc_dag.OpNode, output_name: str, group_col_names: list,
              over_col_name: str, aggregator: str, agg_out_col_name: str):
    """
    Define Aggregate operation.

    :param input_op_node: Parent node for the node returned by this method.
    :param output_name: Name of returned Aggregate node.
    :param group_col_names: List of column names to be used as key columns in the aggregation.
    :param over_col_name: Name of column that gets aggregated.
    :param aggregator: Aggregate function ('+', 'max', 'min', etc.)
    :param agg_out_col_name: Name of (optionally renamed) aggregate column for returned node.
    :return: Aggregate OpNode.
    """

    assert isinstance(group_col_names, list)

    # Get input relation from input node
    in_rel = input_op_node.out_rel

    # Get relevant columns and reset their collusion sets
    in_cols = in_rel.columns
    group_cols = [utils.find(in_cols, group_col_name) for group_col_name in group_col_names]
    over_col = utils.find(in_cols, over_col_name)

    # Create output relation. Default column order is
    # key column first followed by column that will be
    # aggregated. Note that we want copies as these are
    # copies on the output relation and changes to them
    # shouldn't affect the original columns
    agg_out_col = copy.deepcopy(over_col)
    agg_out_col.name = agg_out_col_name
    out_rel_cols = [copy.deepcopy(group_col) for group_col in group_cols]
    out_rel_cols.append(copy.deepcopy(agg_out_col))
    out_rel = rel.Relation(output_name, out_rel_cols, copy.copy(in_rel.stored_with))
    out_rel.update_columns()

    # Create our operator node
    op = cc_dag.Aggregate(out_rel, input_op_node, group_cols, over_col, aggregator)

    # Add it as a child to input node
    input_op_node.children.add(op)

    return op


def aggregate_count(input_op_node: cc_dag.OpNode, output_name: str, group_col_names: list,
                    agg_out_col_name: str):
    """
    Define Aggregate operation.

    :param input_op_node: Parent node for the node returned by this method.
    :param output_name: Name of returned Aggregate node.
    :param group_col_names: List of column names to be used as key columns in the aggregation.
    :param over_col_name: Name of column that gets aggregated.
    :param aggregator: Aggregate function ('+', 'max', 'min', etc.)
    :param agg_out_col_name: Name of (optionally renamed) aggregate column for returned node.
    :return: Aggregate OpNode.
    """

    assert isinstance(group_col_names, list)
    # Get input relation from input node
    in_rel = input_op_node.out_rel

    # Get relevant columns and reset their collusion sets
    in_cols = in_rel.columns
    group_cols = [utils.find(in_cols, group_col_name) for group_col_name in group_col_names]

    # Create output relation. Default column order is
    # key column first followed by column that will be
    # aggregated. Note that we want copies as these are
    # copies on the output relation and changes to them
    # shouldn't affect the original columns
    agg_out_col = Column(output_name, agg_out_col_name, len(group_cols), "INTEGER", set())
    out_rel_cols = [copy.deepcopy(group_col) for group_col in group_cols]
    out_rel_cols.append(copy.deepcopy(agg_out_col))
    out_rel = rel.Relation(output_name, out_rel_cols, copy.copy(in_rel.stored_with))
    out_rel.update_columns()

    # Create our operator node
    op = cc_dag.Aggregate(out_rel, input_op_node, group_cols, None, "count")

    # Add it as a child to input node
    input_op_node.children.add(op)

    return op


def index_aggregate(input_op_node: cc_dag.OpNode, output_name: str, group_col_names: list,
                    over_col_name: str, aggregator: str, agg_out_col_name: str, eq_flag_op, sorted_keys_op):
    """
    Define IndexAggregate operation.

    :param input_op_node: Parent node for the node returned by this method.
    :param output_name: Name of returned IndexAggregate node.
    :param group_col_names: List of column names to be used as key columns in the aggregation.
    :param over_col_name: Name of column that gets aggregated.
    :param aggregator: Aggregate function ('+', 'max', 'min', etc.)
    :param agg_out_col_name: Name of (optionally renamed) aggregate column for returned node.
    :param eq_flag_op: # TODO
    :param sorted_keys_op: # TODO
    :return: IndexAggregate OpNode.
    """

    agg_op = aggregate(input_op_node, output_name, group_col_names, over_col_name, aggregator, agg_out_col_name)
    idx_agg_op = cc_dag.IndexAggregate.from_aggregate(agg_op, eq_flag_op, sorted_keys_op)

    input_op_node.children.remove(agg_op)
    input_op_node.children.add(idx_agg_op)

    eq_flag_op.children.add(idx_agg_op)
    sorted_keys_op.children.add(idx_agg_op)

    idx_agg_op.parents.add(eq_flag_op)
    idx_agg_op.parents.add(sorted_keys_op)

    return idx_agg_op


def _leaky_index_aggregate(input_op_node: cc_dag.OpNode, output_name: str, group_col_names: list, over_col_name: str,
                           aggregator: str, agg_out_col_name: str, dist_keys_op: cc_dag.OpNode,
                           keys_to_idx_map: cc_dag.OpNode):
    """
    Define IndexAggregateLeaky operation.
    """

    agg_op = aggregate(input_op_node, output_name, group_col_names, over_col_name, aggregator, agg_out_col_name)
    idx_agg_op = cc_dag.LeakyIndexAggregate.from_aggregate(agg_op, dist_keys_op, keys_to_idx_map)

    input_op_node.children.remove(agg_op)
    input_op_node.children.add(idx_agg_op)

    dist_keys_op.children.add(idx_agg_op)
    keys_to_idx_map.children.add(idx_agg_op)

    idx_agg_op.parents.add(dist_keys_op)
    idx_agg_op.parents.add(keys_to_idx_map)

    return idx_agg_op


def sort_by(input_op_node: cc_dag.OpNode, output_name: str, sort_by_col_name: str):
    """
    Define SortBy operation.

    :param input_op_node: Parent node for the node returned by this method.
    :param output_name: Name of returned SortBy node.
    :param sort_by_col_name: Name of column that keys sorting.
    :return: SortBy OpNode.
    """

    # Get input relation from input node
    in_rel = input_op_node.out_rel

    # Get relevant columns and create copies
    out_rel_cols = copy.deepcopy(in_rel.columns)

    sort_by_col = utils.find(in_rel.columns, sort_by_col_name)

    for col in out_rel_cols:
        col.trust_set = set()

    # Create output relation
    out_rel = rel.Relation(output_name, out_rel_cols, copy.copy(in_rel.stored_with))
    out_rel.update_columns()

    # Create our operator node
    op = cc_dag.SortBy(out_rel, input_op_node, sort_by_col)

    # Add it as a child to input node
    input_op_node.children.add(op)

    return op


def limit(input_op_node: cc_dag.OpNode, output_name: str, num: int):
    """
    Define Limit operation.
    """

    in_rel = input_op_node.out_rel

    out_rel_cols = copy.deepcopy(in_rel.columns)

    for col in out_rel_cols:
        col.coll_sets = set()

    out_rel = rel.Relation(output_name, out_rel_cols, copy.copy(in_rel.stored_with))
    out_rel.update_columns()

    op = cc_dag.Limit(out_rel, input_op_node, num)

    input_op_node.children.add(op)

    return op


def project(input_op_node: cc_dag.OpNode, output_name: str, selected_col_names: list):
    """
    Define Project operation.

    :param input_op_node: Parent node for the node returned by this method.
    :param output_name: Name of returned Project node.
    :param selected_col_names: List of column names that will be projected from the parent out relation.
    :return: Project OpNode.
    """

    # Get input relation from input node
    in_rel = input_op_node.out_rel

    # Find all columns by name
    selected_cols = [utils.find(in_rel.columns, col_name) for col_name in selected_col_names]

    out_rel_cols = copy.deepcopy(selected_cols)
    for col in out_rel_cols:
        col.trust_set = set()

    # Create output relation
    out_rel = rel.Relation(output_name, out_rel_cols, copy.copy(in_rel.stored_with))
    out_rel.update_columns()

    # Create our operator node
    op = cc_dag.Project(out_rel, input_op_node, selected_cols)

    # Add it as a child to input node
    input_op_node.children.add(op)

    return op


def distinct(input_op_node: cc_dag.OpNode, output_name: str, selected_col_names: list):
    """
    Define Distinct operation.

    :param input_op_node: Parent node for the node returned by this method.
    :param output_name: Name of returned Distinct node.
    :param selected_col_names: List of column names the the Distinct operation will key over.
    :return: Distinct OpNode.
    """

    # Get input relation from input node
    in_rel = input_op_node.out_rel

    # Find all columns by name
    selected_cols = [utils.find(in_rel.columns, col_name) for col_name in selected_col_names]

    out_rel_cols = copy.deepcopy(selected_cols)
    for col in out_rel_cols:
        col.trust_set = set()

    # Create output relation
    out_rel = rel.Relation(output_name, out_rel_cols, copy.copy(in_rel.stored_with))
    out_rel.update_columns()

    # Create our operator node
    op = cc_dag.Distinct(out_rel, input_op_node, selected_cols)

    # Add it as a child to input node
    input_op_node.children.add(op)

    return op


def distinct_count(input_op_node: cc_dag.OpNode, output_name: str, selected_col_name: str):
    """
    Define DistinctCount operation.

    :param input_op_node: Parent node for the node returned by this method.
    :param output_name: Name of returned Distinct node.
    :param selected_col_name: Column name the Distinct operation will key over.
    :param use_sort: flag indicating if sort is necessary or not
    :return: Distinct OpNode.
    """

    # Get input relation from input node
    in_rel = input_op_node.out_rel

    # Find all columns by name
    selected_col = utils.find(in_rel.columns, selected_col_name)

    out_rel_cols = copy.deepcopy([selected_col])
    for col in out_rel_cols:
        col.trust_set = set()

    # Create output relation
    out_rel = rel.Relation(output_name, out_rel_cols, copy.copy(in_rel.stored_with))
    out_rel.update_columns()

    # Create our operator node
    op = cc_dag.DistinctCount(out_rel, input_op_node, selected_col)

    # Add it as a child to input node
    input_op_node.children.add(op)

    return op


def divide(input_op_node: cc_dag.OpNode, output_name: str, target_col_name: str, operands: list):
    """
    Define Divide operation.

    :param input_op_node: Parent node for the node returned by this method.
    :param output_name: Name of returned Divide node.
    :param target_col_name: Name of column that stores results of Divide operation.
    If target_col_name refers to an already existing column in the relation, then that
    column should also be the first argument in the operands list. If target_col_name
    does not refer to an existing column, then the columns in the operands list will
    be divided together in order, and stored in a column named <target_col_name> and
    appended to the relation.
    :param operands: List of operand columns & scalars.
    :return: Divide OpNode.
    """

    # Get input relation from input node
    in_rel = input_op_node.out_rel

    # Get relevant columns and create copies
    out_rel_cols = copy.deepcopy(in_rel.columns)

    # Replace all column names with corresponding columns.
    operands = [utils.find(in_rel.columns, op) if isinstance(op, str) else op for op in operands]

    # if target_col already exists, it will be at the 0th index of operands
    if target_col_name == operands[0].name:
        target_column = utils.find(in_rel.columns, target_col_name)
    else:
        # TODO: figure out new column's trust_set
        target_column = rel.Column(
            output_name, target_col_name, len(in_rel.columns), "INTEGER", set())
        out_rel_cols.append(target_column)

    # Create output relation
    out_rel = rel.Relation(output_name, out_rel_cols, copy.copy(in_rel.stored_with))
    out_rel.update_columns()

    # Create our operator node
    op = cc_dag.Divide(out_rel, input_op_node, target_column, operands)

    # Add it as a child to input node
    input_op_node.children.add(op)

    return op


def filter_by(input_op_node: cc_dag.OpNode, output_name: str, filter_col_name: str, by_op: cc_dag.OpNode,
              use_not_in: bool = False):
    """
    Define FilterBy operation.

    :param input_op_node: Parent node for the node returned by this method.
    :param output_name: Name of returned Filter node.
    :param filter_col_name: Name of column that relation gets filtered over.
    :param by_op: Parent node to filter by.
    :param use_not_in: flag indicating whether to use not in instead of in
    :return: FilterBy OpNode
    """

    # Get input relation from input node
    in_rel = input_op_node.out_rel

    # Get relevant columns and create copies
    out_rel_cols = copy.deepcopy(in_rel.columns)

    # Get index of filter column
    filter_col = utils.find(in_rel.columns, filter_col_name)

    # Create output relation
    out_rel = rel.Relation(output_name, out_rel_cols, copy.copy(in_rel.stored_with))
    out_rel.update_columns()

    # Create our operator node
    op = cc_dag.FilterBy(out_rel, input_op_node, by_op, filter_col, use_not_in)

    # Add it as a child to input nodes
    input_op_node.children.add(op)
    by_op.children.add(op)

    return op


def _indexes_to_flags(input_op_node: cc_dag.OpNode, lookup_op_node: cc_dag.OpNode, output_name: str, stage: int = 0):
    # Get input relation from input node
    in_rel = lookup_op_node.out_rel

    # Get relevant columns and create copies
    out_rel_cols = [copy.deepcopy(in_rel.columns[0])]

    # Create output relation
    out_rel = rel.Relation(output_name, out_rel_cols, copy.copy(in_rel.stored_with))
    out_rel.update_columns()

    # Create our operator node
    op = cc_dag.IndexesToFlags(out_rel, input_op_node, lookup_op_node, stage)

    # Add it as a child to input nodes
    input_op_node.children.add(op)
    lookup_op_node.children.add(op)

    return op


def union(left_input_node: cc_dag.OpNode, right_input_node: cc_dag.OpNode, output_name: str, left_col_name: str,
          right_col_name: str):
    """
    Computes a single column relation containing the union of values of the two selected columns.
    """

    # Create output column and relation
    out_col = Column(output_name, left_col_name, 0, "INTEGER", set())
    left_stored_with = left_input_node.out_rel.stored_with
    right_stored_with = right_input_node.out_rel.stored_with
    out_rel = rel.Relation(output_name, [out_col], left_stored_with.union(right_stored_with))
    out_rel.update_columns()

    left_col = utils.find(left_input_node.out_rel.columns, left_col_name)
    right_col = utils.find(right_input_node.out_rel.columns, right_col_name)
    op = cc_dag.Union(out_rel, left_input_node, right_input_node, left_col, right_col)

    # Add it as a child to input nodes
    left_input_node.children.add(op)
    right_input_node.children.add(op)

    return op


def _pub_intersect(input_node: cc_dag.OpNode,
                   output_name: str,
                   col_name: str,
                   host: str = "ca-spark-node-0",
                   port: int = 8042,
                   is_server: bool = True):
    """
    Computes a single column relation containing the intersection of values of the two selected columns.
    """

    # Create output column and relation
    out_col = Column(output_name, col_name, 0, "INTEGER", set())
    left_stored_with = input_node.out_rel.stored_with
    out_rel = rel.Relation(output_name, [out_col], copy.copy(left_stored_with))
    out_rel.update_columns()

    left_col = utils.find(input_node.out_rel.columns, col_name)
    op = cc_dag.PubIntersect(out_rel, input_node, left_col, host, port, is_server)

    # Add it as a child to input node
    input_node.children.add(op)

    return op


def cc_filter(input_op_node: cc_dag.OpNode, output_name: str, filter_col_name: str, operator: str,
              other_col_name: str = None, scalar: int = None):
    """
    Define Filter operation.

    :param input_op_node: Parent node for the node returned by this method.
    :param output_name: Name of returned Filter node.
    :param filter_col_name: Name of column that relation gets filtered over.
    :param operator: == or <
    :param other_col_name: Name of column to compare to (possibly none).
    :param scalar: Scalar to compare to(possibly none).

    :return: Filter OpNode
    """

    # Make sure we're using valid operator option
    assert operator in {"==", "<"}

    # Get input relation from input node
    in_rel = input_op_node.out_rel

    # Get relevant columns and create copies
    out_rel_cols = copy.deepcopy(in_rel.columns)

    # Get index of filter column
    filter_col = utils.find(in_rel.columns, filter_col_name)

    # Get index of other column (if there is one)
    other_col = utils.find(in_rel.columns, other_col_name) if other_col_name else None

    # Create output relation
    out_rel = rel.Relation(output_name, out_rel_cols, copy.copy(in_rel.stored_with))
    out_rel.update_columns()

    # Create our operator node
    op = cc_dag.Filter(out_rel, input_op_node, filter_col, operator, other_col, scalar)

    # Add it as a child to input node
    input_op_node.children.add(op)

    return op


def multiply(input_op_node: cc_dag.OpNode, output_name: str, target_col_name: str, operands: list):
    """
    Define Multiply operation.

    :param input_op_node: Parent node for the node returned by this method.
    :param output_name: Name of returned Multiply node.
    :param target_col_name: Name of column that stores results of Multiply operation.
    If target_col_name refers to an already existing column in the relation, then that
    column should also be the first argument in the operands list. If target_col_name
    does not refer to an existing column, then the columns in the operands list will
    be multiplied together in order, and stored in a column named <target_col_name> and
    appended to the relation.
    :param operands: List of operand columns & scalars.
    :return: Multiply OpNode.
    """

    # Get input relation from input node
    in_rel = input_op_node.out_rel

    # Get relevant columns and create copies
    out_rel_cols = copy.deepcopy(in_rel.columns)

    # Replace all column names with corresponding columns.
    operands = [utils.find(in_rel.columns, op) if isinstance(op, str) else op for op in operands]

    # if target_col already exists, it will be at the 0th index of operands
    if target_col_name == operands[0].name:
        target_column = utils.find(in_rel.columns, target_col_name)
    else:
        # TODO: figure out new column's trust_set
        target_column = rel.Column(output_name, target_col_name, len(in_rel.columns), "INTEGER", set())
        out_rel_cols.append(target_column)

    # Create output relation
    out_rel = rel.Relation(output_name, out_rel_cols, copy.copy(in_rel.stored_with))
    out_rel.update_columns()

    # Create our operator node
    op = cc_dag.Multiply(out_rel, input_op_node, target_column, operands)

    # Add it as a child to input node
    input_op_node.children.add(op)

    return op


# TODO hack hack hack
def _pub_join(input_op_node: cc_dag.OpNode, output_name: str, key_col_name: str, host: str = "ca-spark-node-0",
              port: int = 8042,
              is_server: bool = True,
              other_op_node: cc_dag.OpNode = None):
    # Get input relation from input node
    in_rel = input_op_node.out_rel

    # Get relevant columns and create copies
    out_rel_cols = copy.deepcopy(in_rel.columns)
    if other_op_node:
        out_rel_cols += copy.deepcopy(other_op_node.out_rel.columns[1:])

    # Get index of filter column
    key_col = utils.find(in_rel.columns, key_col_name)
    assert key_col.idx == 0
    # key_col.trust_set = set()

    # Create output relation
    out_rel = rel.Relation(output_name, out_rel_cols, copy.copy(in_rel.stored_with))
    out_rel.update_columns()

    # Create our operator node
    op = cc_dag.PubJoin(out_rel, input_op_node, key_col, host, port, is_server, other_op_node)

    # Add it as a child to input node
    input_op_node.children.add(op)
    if other_op_node:
        other_op_node.children.add(op)

    return op


def concat_cols(input_op_nodes: list, output_name: str, use_mult=False):
    """Defines operation for combining the columns from multiple relations into one."""
    out_rel_cols = []
    # TODO hack hack hack
    if use_mult:
        out_rel_cols += copy.deepcopy(input_op_nodes[0].out_rel.columns)
    else:
        for input_op_node in input_op_nodes:
            out_rel_cols += copy.deepcopy(input_op_node.out_rel.columns)

    for col in out_rel_cols:
        col.trust_set = set()

    # Get input relations from input nodes
    in_rels = [input_op_node.out_rel for input_op_node in input_op_nodes]

    in_stored_with = [in_rel.stored_with for in_rel in in_rels]
    out_stored_with = set().union(*in_stored_with)

    # Create output relation
    out_rel = rel.Relation(output_name, out_rel_cols, out_stored_with)
    out_rel.update_columns()

    # Create our operator node
    op = cc_dag.ConcatCols(out_rel, input_op_nodes, use_mult)

    # Add it as a child to each input node
    for input_op_node in input_op_nodes:
        input_op_node.children.add(op)

    return op


# TODO: is a self-join a problem?
def join(left_input_node: cc_dag.OpNode, right_input_node: cc_dag.OpNode, output_name: str,
         left_col_names: list, right_col_names: list):
    """
    Define Join operation.

    :param left_input_node: Left parent node for the node returned by this method.
    :param right_input_node: Right parent node for the node returned by this method.
    :param output_name: Name of returned Join node.
    :param left_col_names: List of join columns in left parent relation.
    :param right_col_names: List of join columns in right parent relation.
    :return: Join OpNode.
    """

    # TODO: technically this should take in a start index as well
    # This helper method takes in a relation, the key column of the join
    # and its index.
    # It returns a list of new columns with correctly merged collusion sets
    # for the output relation (in the same order as they appear on the input
    # relation but excluding the key column)
    def _cols_from_rel(start_idx: int, relation: rel.Relation, key_col_idxs: list):

        result_cols = []
        for num, col in enumerate(relation.columns):
            # Exclude key columns and add num from enumerate to start index
            if col.idx not in set(key_col_idxs):
                new_col = rel.Column(
                    output_name, col.get_name(), num + start_idx - len(key_col_idxs), col.type_str, set())
                result_cols.append(new_col)

        return result_cols

    assert isinstance(left_col_names, list)
    assert isinstance(right_col_names, list)

    # Get input relation from input nodes
    left_in_rel = left_input_node.out_rel
    right_in_rel = right_input_node.out_rel

    # Get columns from both relations
    left_cols = left_in_rel.columns
    right_cols = right_in_rel.columns

    # Get columns we will join on
    left_join_cols = [utils.find(left_cols, left_col_name) for left_col_name in left_col_names]
    right_join_cols = [utils.find(right_cols, right_col_name) for right_col_name in right_col_names]

    # Create new key columns
    out_key_cols = []
    for i in range(len(left_join_cols)):
        out_key_cols.append(
            rel.Column(output_name, left_join_cols[i].get_name(), i, left_join_cols[i].type_str, set()))

    # Define output relation columns.
    # These will be the key columns followed
    # by all columns from left (other than join columns)
    # and right (again excluding join columns)

    start_idx = len(out_key_cols)
    # continue_idx will be (start_idx + len(left_in_rel.columns) - len(left_join_cols)),
    # which is just len(left_in_rel.columns)
    continue_idx = len(left_in_rel.columns)
    out_rel_cols = out_key_cols \
                   + _cols_from_rel(
        start_idx, left_in_rel, [left_join_col.idx for left_join_col in left_join_cols]) \
                   + _cols_from_rel(
        continue_idx, right_in_rel, [right_join_col.idx for right_join_col in right_join_cols])

    # The result of the join will be stored with the union
    # of the parties storing left and right
    out_stored_with = left_in_rel.stored_with.union(right_in_rel.stored_with)

    # Create output relation
    out_rel = rel.Relation(output_name, out_rel_cols, out_stored_with)
    out_rel.update_columns()

    # Create join operator
    op = cc_dag.Join(
        out_rel,
        left_input_node,
        right_input_node,
        left_join_cols,
        right_join_cols
    )

    # Add it as a child to both input nodes
    left_input_node.children.add(op)
    right_input_node.children.add(op)

    return op


def concat(input_op_nodes: list, output_name: str, column_names: [list, None] = None):
    """
    Define Concat operation.

    :param input_op_nodes: List of parent nodes for the node returned by this method.
    :param output_name: Name of returned Concat node.
    :param column_names: List of output relation column names.
    :return: Concat OpNode.
    """

    # Make sure we have at least two input node as a
    # sanity check--could relax this in the future
    assert (len(input_op_nodes) >= 2)

    # Get input relations from input nodes
    in_rels = [input_op_node.out_rel for input_op_node in input_op_nodes]

    # Ensure that all input relations have same
    # number of columns
    num_cols = len(in_rels[0].columns)
    for in_rel in in_rels:
        assert (len(in_rel.columns) == num_cols)
    if column_names is not None:
        assert (len(column_names) == num_cols)

    # Copy over columns from existing relation
    out_rel_cols = copy.deepcopy(in_rels[0].columns)
    for (i, col) in enumerate(out_rel_cols):
        if column_names is not None:
            col.name = column_names[i]
        else:
            # we use the column names from the first input
            pass
        col.trust_set = set()

    # The result of the concat will be stored with the union
    # of the parties storing the input relations
    in_stored_with = [in_rel.stored_with for in_rel in in_rels]
    out_stored_with = set().union(*in_stored_with)

    # Create output relation
    out_rel = rel.Relation(output_name, out_rel_cols, out_stored_with)
    out_rel.update_columns()

    # Create our operator node
    op = cc_dag.Concat(out_rel, input_op_nodes)

    # Add it as a child to each input node
    for input_op_node in input_op_nodes:
        input_op_node.children.add(op)

    return op


def blackbox(input_op_nodes: list, output_name: str, column_names: list, backend: str, code: str):
    # Get input relations from input nodes
    in_rels = [input_op_node.out_rel for input_op_node in input_op_nodes]

    # Create output columns
    out_columns = [Column(output_name, col_name, idx, "INTEGER", set()) for idx, col_name in enumerate(column_names)]

    # Create out rel
    in_stored_with = [in_rel.stored_with for in_rel in in_rels]
    out_stored_with = set().union(*in_stored_with)
    out_rel = rel.Relation(output_name, out_columns, out_stored_with)

    # Create our operator node
    op = cc_dag.Blackbox(out_rel, input_op_nodes, backend, code)

    # Add it as a child to each input node
    for input_op_node in input_op_nodes:
        input_op_node.children.add(op)

    return op


def index(input_op_node: cc_dag.OpNode, output_name: str, idx_col_name: str = "index"):
    """
    Define Index operation.

    :param input_op_node: Parent node for the node returned by this method.
    :param output_name: Name of returned Index node.
    :param idx_col_name: Name of index column that gets appended to relation.
    :return: Index OpNode.
    """

    in_rel = input_op_node.out_rel

    # Copy over columns from existing relation
    out_rel_cols = copy.deepcopy(in_rel.columns)

    # TODO should be 0?
    index_col = rel.Column(
        output_name, idx_col_name, len(in_rel.columns), "INTEGER", set())
    out_rel_cols = [index_col] + out_rel_cols

    # Create output relation
    out_rel = rel.Relation(output_name, out_rel_cols, copy.copy(in_rel.stored_with))
    out_rel.update_columns()

    op = cc_dag.Index(out_rel, input_op_node, idx_col_name)
    # Add it as a child to input node
    input_op_node.children.add(op)

    return op


def num_rows(input_op_node: cc_dag.OpNode, output_name: str, col_name: str = "len"):
    # TODO docs
    in_rel = input_op_node.out_rel
    # Copy over columns from existing relation
    len_col = rel.Column(
        output_name, col_name, len(in_rel.columns), "INTEGER", set())
    out_rel_cols = [len_col]

    # Create output relation
    out_rel = rel.Relation(output_name, out_rel_cols, copy.copy(in_rel.stored_with))
    out_rel.update_columns()

    op = cc_dag.NumRows(out_rel, input_op_node, len_col)
    # Add it as a child to input node
    input_op_node.children.add(op)

    return op


def _unpermute(input_op_node: cc_dag.OpNode, output_name: str, sort_by_col_name: str, re_indexed_col_name: str):
    """
    Given indexes, re-indexes, and sorts by original indexes.
    """
    # TODO move this and other internal functions somewhere more appropriate
    re_indexed = index(input_op_node, input_op_node.out_rel.name + "_reidx", re_indexed_col_name)
    re_indexed.is_mpc = False
    unpermuted = sort_by(re_indexed, output_name, sort_by_col_name)
    re_indexed.is_mpc = False
    return unpermuted


def shuffle(input_op_node: cc_dag.OpNode, output_name: str):
    """
    Define Shuffle operation.

    :param input_op_node: Parent node for the node returned by this method.
    :param output_name: Name of returned Shuffle node.
    :return: Shuffle OpNode.
    """

    in_rel = input_op_node.out_rel

    # Copy over columns from existing relation
    out_rel_cols = copy.deepcopy(in_rel.columns)

    # Create output relation
    out_rel = rel.Relation(output_name, out_rel_cols, copy.copy(in_rel.stored_with))
    out_rel.update_columns()

    op = cc_dag.Shuffle(out_rel, input_op_node)
    # Add it as a child to input node
    input_op_node.children.add(op)

    return op


def collect(input_op_node: cc_dag.OpNode, target_party: int):
    """
    Define Collect operation.

    :param input_op_node: Parent node for the node returned by this method.
    :param target_party: PID of party that receives outputs.
    :return: Collect OpNode.
    """

    # Get input relation from input node
    in_rel = input_op_node.out_rel
    in_rel.stored_with = {target_party}


# Below functions are NOT part of the public API! Only used to simplify codegen testing

def _comp_neighs(input_op_node: cc_dag.OpNode, output_name: str, comp_col_name: str):
    """
    Define CompNeighs operation.

    :param input_op_node: Parent node for the node returned by this method.
    :param output_name: Name of returned CompNeighs node.
    :param comp_col_name: Name of column that keys comparison operation.
    :return: CompNeighs OpNode.
    """

    # Get input relation from input node
    in_rel = input_op_node.out_rel

    # Get relevant columns and create copies
    out_rel_cols = copy.deepcopy(in_rel.columns)

    comp_col = utils.find(in_rel.columns, comp_col_name)
    comp_col.stored_with = set()

    for col in out_rel_cols:
        col.trust_set = set()

    # Create output relation
    out_rel = rel.Relation(output_name, [copy.deepcopy(comp_col)], copy.copy(in_rel.stored_with))
    out_rel.update_columns()

    # Create our operator node
    op = cc_dag.CompNeighs(out_rel, input_op_node, comp_col)

    # Add it as a child to input node
    input_op_node.children.add(op)

    return op


def _index_join(left_input_node: cc_dag.OpNode, right_input_node: cc_dag.OpNode, output_name: str,
                left_col_names: list, right_col_names: list, index_op_node: cc_dag.Index):
    """
    Define IndexJoin operation.

    :param left_input_node: Left parent node for the node returned by this method.
    :param right_input_node: Right parent node for the node returned by this method.
    :param output_name: Name of returned IndexJoin node.
    :param left_col_names: List of join columns in left parent relation.
    :param right_col_names: List of join columns in right parent relation.
    :param index_op_node: Index node that gets combined with Join relation.
    :return: IndexJoin OpNode.
    """

    join_op = join(left_input_node, right_input_node,
                   output_name, left_col_names, right_col_names)
    idx_join_op = cc_dag.IndexJoin.from_join(join_op, index_op_node)

    left_input_node.children.remove(join_op)
    right_input_node.children.remove(join_op)

    left_input_node.children.add(idx_join_op)
    right_input_node.children.add(idx_join_op)
    index_op_node.children.add(idx_join_op)

    return idx_join_op


def _persist(input_op_node: cc_dag.OpNode, output_name: str):
    """
    Define Persist operation.

    :param input_op_node: Parent node for the node returned by this method.
    :param output_name: Name of returned Persist node.
    :return: Persist OpNode.
    """

    out_rel = copy.deepcopy(input_op_node.out_rel)
    out_rel.rename(output_name)
    persist_op = cc_dag.Persist(out_rel, input_op_node)
    input_op_node.children.add(persist_op)
    return persist_op


def _close(input_op_node: cc_dag.OpNode, output_name: str, target_parties: set):
    """
    Define Close operation.

    :param input_op_node: Parent node for the node returned by this method.
    :param output_name: Name of returned Close node.
    :param target_parties: ID's of parties that will receive secret shares outputted by this operation.
    :return: Close OpNode.
    """

    out_rel = copy.deepcopy(input_op_node.out_rel)
    out_rel.stored_with = target_parties
    out_rel.rename(output_name)
    close_op = cc_dag.Close(out_rel, input_op_node)
    input_op_node.children.add(close_op)
    return close_op


def _open(input_op_node: cc_dag.OpNode, output_name: str, target_party: int):
    """
    Define Open operation.

    :param input_op_node: Parent node for the node returned by this method.
    :param output_name: Name of returned Open node.
    :param target_party: ID of party that will receive outputs of this operation.
    :return: Open OpNode.
    """

    out_rel = copy.deepcopy(input_op_node.out_rel)
    out_rel.stored_with = {target_party}
    out_rel.rename(output_name)
    open_op = cc_dag.Open(out_rel, input_op_node)
    input_op_node.children.add(open_op)
    return open_op


def _join_flags(left_input_node: cc_dag.OpNode, right_input_node: cc_dag.OpNode, output_name: str,
                left_col_names: list, right_col_names: list, out_col_name: str = "flags"):
    """
    Define JoinFlags operation.
    """

    join_op = join(left_input_node, right_input_node,
                   output_name, left_col_names, right_col_names)
    join_flags_op = cc_dag.JoinFlags.from_join(join_op)
    out_col = rel.Column(output_name, out_col_name, 0, "INTEGER", join_flags_op.out_rel.columns[0].trust_set)
    out_rel = rel.Relation(output_name, [out_col], join_flags_op.out_rel.stored_with)
    join_flags_op.out_rel = out_rel
    join_flags_op.is_mpc = True

    left_input_node.children.remove(join_op)
    right_input_node.children.remove(join_op)

    left_input_node.children.add(join_flags_op)
    right_input_node.children.add(join_flags_op)

    return join_flags_op


def _flag_join(left_input_node: cc_dag.OpNode, right_input_node: cc_dag.OpNode, output_name: str,
               left_col_names: list, right_col_names: list, join_flags_op_node: cc_dag.OpNode):
    """
    Define FlagJoin operation.
    TODO rename
    """
    node_out_rel = left_input_node.out_rel
    out_rel = rel.Relation(output_name, copy.deepcopy(node_out_rel.columns), copy.copy(node_out_rel.stored_with))
    flag_join_op = cc_dag.FlagJoin(out_rel, left_input_node, right_input_node, [], [], join_flags_op_node)

    left_input_node.children.add(flag_join_op)
    right_input_node.children.add(flag_join_op)
    join_flags_op_node.children.add(flag_join_op)

    return flag_join_op
