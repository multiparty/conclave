"""
Embedded language for relational workflows.
"""
import copy
from salmon import rel
from salmon import dag
import salmon.utils as utils


def create(rel_name, columns, stored_with):

    columns = [rel.Column(rel_name, col_name, idx, type_str, collusion_set)
               for idx, (col_name, type_str, collusion_set) in enumerate(columns)]
    out_rel = rel.Relation(rel_name, columns, stored_with)
    op = dag.Create(out_rel)
    return op


def aggregate(input_op_node, output_name, group_col_names,
              over_col_name, aggregator, agg_out_col_name):

    assert isinstance(group_col_names, list)
    # Get input relation from input node
    in_rel = input_op_node.out_rel

    # Get relevant columns and reset their collusion sets
    in_cols = in_rel.columns
    group_cols = [utils.find(in_cols, group_col_name)
                  for group_col_name in group_col_names]
    for group_col in group_cols:
        group_col.coll_sets = set()
    over_col = utils.find(in_cols, over_col_name)
    over_col.coll_sets = set()

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
    op = dag.Aggregate(out_rel, input_op_node, group_cols, over_col, aggregator)

    # Add it as a child to input node
    input_op_node.children.add(op)

    return op


def index_aggregate(input_op_node, output_name, group_col_names, over_col_name,
                    aggregator, agg_out_col_name, eq_flag_op, sorted_keys_op):

    agg_op = aggregate(input_op_node, output_name, group_col_names,
                       over_col_name, aggregator, agg_out_col_name)
    idx_agg_op = dag.IndexAggregate.from_aggregate(agg_op, eq_flag_op, sorted_keys_op)

    input_op_node.children.remove(agg_op)
    input_op_node.children.add(idx_agg_op)
    
    eq_flag_op.children.add(idx_agg_op)
    sorted_keys_op.children.add(idx_agg_op)

    idx_agg_op.parents.add(eq_flag_op)
    idx_agg_op.parents.add(sorted_keys_op)

    return idx_agg_op


def sort_by(input_op_node, output_name, sort_by_col_name):

    # Get input relation from input node
    in_rel = input_op_node.out_rel

    # Get relevant columns and create copies
    out_rel_cols = copy.deepcopy(in_rel.columns)

    sort_by_col = utils.find(in_rel.columns, sort_by_col_name)

    for col in out_rel_cols:
        col.coll_sets = set()

    # Create output relation
    out_rel = rel.Relation(output_name, out_rel_cols, copy.copy(in_rel.stored_with))
    out_rel.update_columns()

    # Create our operator node
    op = dag.SortBy(out_rel, input_op_node, sort_by_col)

    # Add it as a child to input node
    input_op_node.children.add(op)

    return op


def project(input_op_node, output_name, selected_col_names):

    # Get input relation from input node
    in_rel = input_op_node.out_rel

    # Get relevant columns and create copies
    out_rel_cols = copy.deepcopy(in_rel.columns)

    # Find all columns by name
    selected_cols = [utils.find(in_rel.columns, col_name)
                     for col_name in selected_col_names]

    out_rel_cols = copy.deepcopy(selected_cols)
    for col in out_rel_cols:
        col.coll_sets = set()

    # Create output relation
    out_rel = rel.Relation(output_name, out_rel_cols, copy.copy(in_rel.stored_with))
    out_rel.update_columns()

    # Create our operator node
    op = dag.Project(out_rel, input_op_node, selected_cols)

    # Add it as a child to input node
    input_op_node.children.add(op)

    return op


def distinct(input_op_node, output_name, selected_col_names):

    # Get input relation from input node
    in_rel = input_op_node.out_rel

    # Get relevant columns and create copies
    out_rel_cols = copy.deepcopy(in_rel.columns)

    # Find all columns by name
    selected_cols = [utils.find(in_rel.columns, col_name)
                     for col_name in selected_col_names]

    out_rel_cols = copy.deepcopy(selected_cols)
    for col in out_rel_cols:
        col.coll_sets = set()

    # Create output relation
    out_rel = rel.Relation(output_name, out_rel_cols, copy.copy(in_rel.stored_with))
    out_rel.update_columns()

    # Create our operator node
    op = dag.Distinct(out_rel, input_op_node, selected_cols)

    # Add it as a child to input node
    input_op_node.children.add(op)

    return op


def divide(input_op_node, output_name, target_col_name, operands):

    # Get input relation from input node
    in_rel = input_op_node.out_rel

    # Get relevant columns and create copies
    out_rel_cols = copy.deepcopy(in_rel.columns)

    # Replace all column names with corresponding columns.
    operands = [utils.find(in_rel.columns, op) if isinstance(
        op, str) else op for op in operands]
    for operand in operands:
        if hasattr(operand, "coll_sets"):
            operand.coll_sets = set()

    # if target_col already exists, it will be at the 0th index of operands
    if target_col_name == operands[0].name:
        targetColumn = utils.find(in_rel.columns, target_col_name)
        targetColumn.coll_sets = set()
    else:
        # TODO: figure out new column's coll_sets
        targetColumn = rel.Column(
            output_name, target_col_name, len(in_rel.columns), "INTEGER", set())
        out_rel_cols.append(targetColumn)

    # Create output relation
    out_rel = rel.Relation(output_name, out_rel_cols, copy.copy(in_rel.stored_with))
    out_rel.update_columns()

    # Create our operator node
    op = dag.Divide(out_rel, input_op_node, targetColumn, operands)

    # Add it as a child to input node
    input_op_node.children.add(op)

    return op


def filter(input_op_node, output_name, filterColName, operator, filter_expr):

    # Get input relation from input node
    in_rel = input_op_node.out_rel

    # Get relevant columns and create copies
    out_rel_cols = copy.deepcopy(in_rel.columns)

    # Get index of filter column
    filterCol = utils.find(in_rel.columns, filterColName)
    filterCol.coll_sets = set()

    # Create output relation
    out_rel = rel.Relation(output_name, out_rel_cols, copy.copy(in_rel.stored_with))
    out_rel.update_columns()

    # Create our operator node
    op = dag.Filter(out_rel, input_op_node, filterCol, operator, filter_expr)

    # Add it as a child to input node
    input_op_node.children.add(op)

    return op


def multiply(input_op_node, output_name, target_col_name, operands):

    # Get input relation from input node
    in_rel = input_op_node.out_rel

    # Get relevant columns and create copies
    out_rel_cols = copy.deepcopy(in_rel.columns)

    # Replace all column names with corresponding columns.
    operands = [utils.find(in_rel.columns, op) if isinstance(
        op, str) else op for op in operands]
    for operand in operands:
        if hasattr(operand, "coll_sets"):
            operand.coll_sets = set()

    # if target_col already exists, it will be at the 0th index of operands
    if target_col_name == operands[0].name:
        targetColumn = utils.find(in_rel.columns, target_col_name)
        targetColumn.coll_sets = set()
    else:
        # TODO: figure out new column's coll_sets
        targetColumn = rel.Column(
            output_name, target_col_name, len(in_rel.columns), "INTEGER", set())
        out_rel_cols.append(targetColumn)

    # Create output relation
    out_rel = rel.Relation(output_name, out_rel_cols, copy.copy(in_rel.stored_with))
    out_rel.update_columns()

    # Create our operator node
    op = dag.Multiply(out_rel, input_op_node, targetColumn, operands)

    # Add it as a child to input node
    input_op_node.children.add(op)

    return op


# TODO: is a self-join a problem?
def join(leftInputNode, rightInputNode, output_name, leftColNames, rightColNames):

    # TODO: technically this should take in a start index as well
    # This helper method takes in a relation, the key column of the join
    # and its index.
    # It returns a list of new columns with correctly merged collusion sets
    # for the output relation (in the same order as they appear on the input
    # relation but excluding the key column)
    def _colsFromRel(startIdx, relation, keyColIdxs):

        resultCols = []
        for num, col in enumerate(relation.columns):
            # Exclude key columns and add num from enumerate to start index
            if col.idx not in set(keyColIdxs):
                newCol = rel.Column(
                    output_name, col.getName(), num + startIdx - len(keyColIdxs), col.type_str, set())
                resultCols.append(newCol)

        return resultCols

    assert isinstance(leftColNames, list)
    assert isinstance(rightColNames, list)

    # Get input relation from input nodes
    leftInRel = leftInputNode.out_rel
    rightInRel = rightInputNode.out_rel

    # Get columns from both relations
    leftCols = leftInRel.columns
    rightCols = rightInRel.columns

    # Get columns we will join on
    left_join_cols = [utils.find(leftCols, leftColName)
                    for leftColName in leftColNames]
    right_join_cols = [utils.find(rightCols, rightColName)
                     for rightColName in rightColNames]

    # # Get the key columns' merged collusion set
    # keyCollusionSet = utils.mergeCollusionSets(
    #     left_join_col.collusion_set, right_join_col.collusion_set)

    # Create new key columns
    outKeyCols = []
    for i in range(len(left_join_cols)):
        outKeyCols.append(
            rel.Column(output_name, left_join_cols[i].getName(), i, left_join_cols[i].type_str, set()))

    # Define output relation columns.
    # These will be the key columns followed
    # by all columns from left (other than join columns)
    # and right (again excluding join columns)

    startIdx = len(outKeyCols)
    # continueIdx will be (startIdx + len(leftInRel.columns) - len(left_join_cols)),
    # which is just len(leftInRel.columns)
    continueIdx = len(leftInRel.columns)
    out_rel_cols = outKeyCols \
        + _colsFromRel(
            startIdx, leftInRel, [left_join_col.idx for left_join_col in left_join_cols]) \
        + _colsFromRel(
            continueIdx, rightInRel, [right_join_col.idx for right_join_col in right_join_cols])

    # The result of the join will be stored with the union
    # of the parties storing left and right
    outStoredWith = leftInRel.stored_with.union(rightInRel.stored_with)

    # Create output relation
    out_rel = rel.Relation(output_name, out_rel_cols, outStoredWith)
    out_rel.update_columns()

    # Create join operator
    op = dag.Join(
        out_rel,
        leftInputNode,
        rightInputNode,
        left_join_cols,
        right_join_cols
    )

    # Add it as a child to both input nodes
    leftInputNode.children.add(op)
    rightInputNode.children.add(op)

    return op


def concat(input_op_nodes, output_name, columnNames=None):

    # Make sure we have at least two input node as a
    # sanity check--could relax this in the future
    assert(len(input_op_nodes) >= 2)

    # Get input relations from input nodes
    in_rels = [input_op_node.out_rel for input_op_node in input_op_nodes]

    # Ensure that all input relations have same
    # number of columns
    numCols = len(in_rels[0].columns)
    for in_rel in in_rels:
        assert(len(in_rel.columns) == numCols)
    if columnNames is not None:
        assert(len(columnNames) == numCols)

    # Copy over columns from existing relation
    out_rel_cols = copy.deepcopy(in_rels[0].columns)
    for (i, col) in enumerate(out_rel_cols):
        if columnNames is not None:
            col.name = columnNames[i]
        else:
            # we use the column names from the first input
            pass
        col.coll_sets = set()

    # The result of the concat will be stored with the union
    # of the parties storing the input relations
    inStoredWith = [in_rel.stored_with for in_rel in in_rels]
    outStoredWith = set().union(*inStoredWith)

    # Create output relation
    out_rel = rel.Relation(output_name, out_rel_cols, outStoredWith)
    out_rel.update_columns()

    # Create our operator node
    op = dag.Concat(out_rel, input_op_nodes)

    # Add it as a child to each input node
    for input_op_node in input_op_nodes:
        input_op_node.children.add(op)

    return op


def index(input_op_node, output_name, idxColName="index"):

    in_rel = input_op_node.out_rel

    # Copy over columns from existing relation
    out_rel_cols = copy.deepcopy(in_rel.columns)

    indexCol = rel.Column(
        output_name, idxColName, len(in_rel.columns), "INTEGER", set())
    out_rel_cols = [indexCol] + out_rel_cols

    # Create output relation
    out_rel = rel.Relation(output_name, out_rel_cols, copy.copy(in_rel.stored_with))
    out_rel.update_columns()

    op = dag.Index(out_rel, input_op_node)
    # Add it as a child to input node
    input_op_node.children.add(op)

    return op


def shuffle(input_op_node, output_name):

    in_rel = input_op_node.out_rel

    # Copy over columns from existing relation
    out_rel_cols = copy.deepcopy(in_rel.columns)

    # Create output relation
    out_rel = rel.Relation(output_name, out_rel_cols, copy.copy(in_rel.stored_with))
    out_rel.update_columns()

    op = dag.Shuffle(out_rel, input_op_node)
    # Add it as a child to input node
    input_op_node.children.add(op)

    return op


def collect(input_op_node, targetParty):

    # Get input relation from input node
    in_rel = input_op_node.out_rel
    in_rel.stored_with = set([targetParty])


# Below functions are NOT part of the public API! Only used to simplify
# codegen testing

def _comp_neighs(input_op_node, output_name, compColName):

    # Get input relation from input node
    in_rel = input_op_node.out_rel

    # Get relevant columns and create copies
    out_rel_cols = copy.deepcopy(in_rel.columns)

    compCol = utils.find(in_rel.columns, compColName)
    compCol.stored_with = set()

    for col in out_rel_cols:
        col.coll_sets = set()

    # Create output relation
    out_rel = rel.Relation(output_name, [copy.deepcopy(compCol)], copy.copy(in_rel.stored_with))
    out_rel.update_columns()

    # Create our operator node
    op = dag.CompNeighs(out_rel, input_op_node, compCol)

    # Add it as a child to input node
    input_op_node.children.add(op)

    return op

def _index_join(leftInputNode, rightInputNode, output_name, leftColNames, rightColNames, indexOpNode):

    join_op = join(leftInputNode, rightInputNode,
                   output_name, leftColNames, rightColNames)
    idx_join_op = dag.IndexJoin.from_join(join_op, indexOpNode)

    leftInputNode.children.remove(join_op)
    rightInputNode.children.remove(join_op)

    leftInputNode.children.add(idx_join_op)
    rightInputNode.children.add(idx_join_op)
    indexOpNode.children.add(idx_join_op)

    return idx_join_op


def _persist(input_op_node, output_name):

    out_rel = copy.deepcopy(input_op_node.out_rel)
    out_rel.rename(output_name)
    persistOp = dag.Persist(out_rel, input_op_node)
    input_op_node.children.add(persistOp)
    return persistOp


def _close(input_op_node, output_name, targetParties):

    out_rel = copy.deepcopy(input_op_node.out_rel)
    out_rel.stored_with = targetParties
    out_rel.rename(output_name)
    closeOp = dag.Close(out_rel, input_op_node)
    input_op_node.children.add(closeOp)
    return closeOp


def _open(input_op_node, output_name, targetParty):

    out_rel = copy.deepcopy(input_op_node.out_rel)
    out_rel.stored_with = set([targetParty])
    out_rel.rename(output_name)
    openOp = dag.Open(out_rel, input_op_node)
    input_op_node.children.add(openOp)
    return openOp
