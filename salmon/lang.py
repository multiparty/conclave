"""
Embedded language for relational workflows.
"""
import copy
from salmon import rel
import salmon.dag as saldag
import salmon.utils as utils


def create(rel_name: str, columns: list, stored_with: set):

    columns = [rel.Column(rel_name, col_name, idx, type_str, collusion_set)
               for idx, (col_name, type_str, collusion_set) in enumerate(columns)]
    out_rel = rel.Relation(rel_name, columns, stored_with)
    op = saldag.Create(out_rel)
    return op


def aggregate(input_op_node: saldag.OpNode, output_name: str, group_col_names: list,
              over_col_name: str, aggregator: str, agg_out_col_name: str):

    assert isinstance(group_col_names, list)
    # Get input relation from input node
    in_rel = input_op_node.out_rel

    # Get relevant columns and reset their collusion sets
    in_cols = in_rel.columns
    group_cols = [utils.find(in_cols, group_col_name) for group_col_name in group_col_names]
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
    op = saldag.Aggregate(out_rel, input_op_node, group_cols, over_col, aggregator)

    # Add it as a child to input node
    input_op_node.children.add(op)

    return op


# TODO: (ben) type annotations for eq_flags_op and sorted_keys_op
def index_aggregate(input_op_node: saldag.OpNode, output_name: str, group_col_names: list,
                    over_col_name: str, aggregator: str, agg_out_col_name: str, eq_flag_op, sorted_keys_op):

    agg_op = aggregate(input_op_node, output_name, group_col_names, over_col_name, aggregator, agg_out_col_name)
    idx_agg_op = saldag.IndexAggregate.from_aggregate(agg_op, eq_flag_op, sorted_keys_op)

    input_op_node.children.remove(agg_op)
    input_op_node.children.add(idx_agg_op)
    
    eq_flag_op.children.add(idx_agg_op)
    sorted_keys_op.children.add(idx_agg_op)

    idx_agg_op.parents.add(eq_flag_op)
    idx_agg_op.parents.add(sorted_keys_op)

    return idx_agg_op


def sort_by(input_op_node: saldag.OpNode, output_name: str, sort_by_col_name: str):

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
    op = saldag.SortBy(out_rel, input_op_node, sort_by_col)

    # Add it as a child to input node
    input_op_node.children.add(op)

    return op


def project(input_op_node: saldag.OpNode, output_name: str, selected_col_names: list):

    # Get input relation from input node
    in_rel = input_op_node.out_rel

    # Find all columns by name
    selected_cols = [utils.find(in_rel.columns, col_name) for col_name in selected_col_names]

    out_rel_cols = copy.deepcopy(selected_cols)
    for col in out_rel_cols:
        col.coll_sets = set()

    # Create output relation
    out_rel = rel.Relation(output_name, out_rel_cols, copy.copy(in_rel.stored_with))
    out_rel.update_columns()

    # Create our operator node
    op = saldag.Project(out_rel, input_op_node, selected_cols)

    # Add it as a child to input node
    input_op_node.children.add(op)

    return op


def distinct(input_op_node: saldag.OpNode, output_name: str, selected_col_names: list):

    # Get input relation from input node
    in_rel = input_op_node.out_rel

    # Find all columns by name
    selected_cols = [utils.find(in_rel.columns, col_name) for col_name in selected_col_names]

    out_rel_cols = copy.deepcopy(selected_cols)
    for col in out_rel_cols:
        col.coll_sets = set()

    # Create output relation
    out_rel = rel.Relation(output_name, out_rel_cols, copy.copy(in_rel.stored_with))
    out_rel.update_columns()

    # Create our operator node
    op = saldag.Distinct(out_rel, input_op_node, selected_cols)

    # Add it as a child to input node
    input_op_node.children.add(op)

    return op


def divide(input_op_node, outputName, targetColName, operands):

    # Get input relation from input node
    inRel = input_op_node.out_rel

    # Get relevant columns and create copies
    out_relCols = copy.deepcopy(inRel.columns)

    # Replace all column names with corresponding columns.
    operands = [utils.find(inRel.columns, op) if isinstance(
        op, str) else op for op in operands]
    for operand in operands:
        if hasattr(operand, "coll_sets"):
            operand.collSets = set()

    # if target_col already exists, it will be at the 0th index of operands
    if targetColName == operands[0].name:
        targetColumn = utils.find(inRel.columns, targetColName)
        targetColumn.collSets = set()
    else:
        # TODO: figure out new column's coll_sets
        targetColumn = rel.Column(
            outputName, targetColName, len(inRel.columns), "INTEGER", set())
        out_relCols.append(targetColumn)

    # Create output relation
    out_rel = rel.Relation(outputName, out_relCols, copy.copy(inRel.storedWith))
    out_rel.update_columns()

    # Create our operator node
    op = dag.Divide(out_rel, input_op_node, targetColumn, operands)

    # Add it as a child to input node
    input_op_node.children.add(op)

    return op


def filter(inputOpNode, outputName, filterColName, operator, filterExpr):

    # Get input relation from input node
    inRel = inputOpNode.out_rel

    # Get relevant columns and create copies
    out_relCols = copy.deepcopy(inRel.columns)

    # Get index of filter column
    filterCol = utils.find(inRel.columns, filterColName)
    filterCol.collSets = set()

    # Create output relation
    out_rel = rel.Relation(outputName, out_relCols, copy.copy(inRel.storedWith))
    out_rel.update_columns()

    # Create our operator node
    op = dag.Filter(out_rel, inputOpNode, filterCol, operator, filterExpr)

    # Add it as a child to input node
    inputOpNode.children.add(op)

    return op


def multiply(inputOpNode, outputName, targetColName, operands):

    # Get input relation from input node
    inRel = inputOpNode.out_rel

    # Get relevant columns and create copies
    out_relCols = copy.deepcopy(inRel.columns)

    # Replace all column names with corresponding columns.
    operands = [utils.find(inRel.columns, op) if isinstance(
        op, str) else op for op in operands]
    for operand in operands:
        if hasattr(operand, "coll_sets"):
            operand.collSets = set()

    # if target_col already exists, it will be at the 0th index of operands
    if targetColName == operands[0].name:
        targetColumn = utils.find(inRel.columns, targetColName)
        targetColumn.collSets = set()
    else:
        # TODO: figure out new column's coll_sets
        targetColumn = rel.Column(
            outputName, targetColName, len(inRel.columns), "INTEGER", set())
        out_relCols.append(targetColumn)

    # Create output relation
    out_rel = rel.Relation(outputName, out_relCols, copy.copy(inRel.storedWith))
    out_rel.update_columns()

    # Create our operator node
    op = dag.Multiply(out_rel, inputOpNode, targetColumn, operands)

    # Add it as a child to input node
    inputOpNode.children.add(op)

    return op


# TODO: is a self-join a problem?
def join(leftInputNode, rightInputNode, outputName, leftColNames, rightColNames):

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
                    outputName, col.get_name(), num + startIdx - len(keyColIdxs), col.typeStr, set())
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
    leftJoinCols = [utils.find(leftCols, leftColName)
                    for leftColName in leftColNames]
    rightJoinCols = [utils.find(rightCols, rightColName)
                     for rightColName in rightColNames]

    # # Get the key columns' merged collusion set
    # keyCollusionSet = utils.mergeCollusionSets(
    #     leftJoinCol.collusionSet, rightJoinCol.collusionSet)

    # Create new key columns
    outKeyCols = []
    for i in range(len(leftJoinCols)):
        outKeyCols.append(
            rel.Column(outputName, leftJoinCols[i].get_name(), i, leftJoinCols[i].typeStr, set()))

    # Define output relation columns.
    # These will be the key columns followed
    # by all columns from left (other than join columns)
    # and right (again excluding join columns)

    startIdx = len(outKeyCols)
    # continueIdx will be (startIdx + len(leftInRel.columns) - len(left_join_cols)),
    # which is just len(leftInRel.columns)
    continueIdx = len(leftInRel.columns)
    out_relCols = outKeyCols \
        + _colsFromRel(
            startIdx, leftInRel, [leftJoinCol.idx for leftJoinCol in leftJoinCols]) \
        + _colsFromRel(
            continueIdx, rightInRel, [rightJoinCol.idx for rightJoinCol in rightJoinCols])

    # The result of the join will be stored with the union
    # of the parties storing left and right
    outStoredWith = leftInRel.storedWith.union(rightInRel.storedWith)

    # Create output relation
    out_rel = rel.Relation(outputName, out_relCols, outStoredWith)
    out_rel.update_columns()

    # Create join operator
    op = dag.Join(
        out_rel,
        leftInputNode,
        rightInputNode,
        leftJoinCols,
        rightJoinCols
    )

    # Add it as a child to both input nodes
    leftInputNode.children.add(op)
    rightInputNode.children.add(op)

    return op


def concat(inputOpNodes, outputName, columnNames=None):

    # Make sure we have at least two input node as a
    # sanity check--could relax this in the future
    assert(len(inputOpNodes) >= 2)

    # Get input relations from input nodes
    inRels = [inputOpNode.out_rel for inputOpNode in inputOpNodes]

    # Ensure that all input relations have same
    # number of columns
    numCols = len(inRels[0].columns)
    for inRel in inRels:
        assert(len(inRel.columns) == numCols)
    if columnNames is not None:
        assert(len(columnNames) == numCols)

    # Copy over columns from existing relation
    out_relCols = copy.deepcopy(inRels[0].columns)
    for (i, col) in enumerate(out_relCols):
        if columnNames is not None:
            col.name = columnNames[i]
        else:
            # we use the column names from the first input
            pass
        col.collSets = set()

    # The result of the concat will be stored with the union
    # of the parties storing the input relations
    inStoredWith = [inRel.storedWith for inRel in inRels]
    outStoredWith = set().union(*inStoredWith)

    # Create output relation
    out_rel = rel.Relation(outputName, out_relCols, outStoredWith)
    out_rel.update_columns()

    # Create our operator node
    op = dag.Concat(out_rel, inputOpNodes)

    # Add it as a child to each input node
    for inputOpNode in inputOpNodes:
        inputOpNode.children.add(op)

    return op


def index(inputOpNode, outputName, idxColName="index"):

    inRel = inputOpNode.out_rel

    # Copy over columns from existing relation
    out_relCols = copy.deepcopy(inRel.columns)

    indexCol = rel.Column(
        outputName, idxColName, len(inRel.columns), "INTEGER", set())
    out_relCols = [indexCol] + out_relCols

    # Create output relation
    out_rel = rel.Relation(outputName, out_relCols, copy.copy(inRel.storedWith))
    out_rel.update_columns()

    op = dag.Index(out_rel, inputOpNode)
    # Add it as a child to input node
    inputOpNode.children.add(op)

    return op


def shuffle(inputOpNode, outputName):

    inRel = inputOpNode.out_rel

    # Copy over columns from existing relation
    out_relCols = copy.deepcopy(inRel.columns)

    # Create output relation
    out_rel = rel.Relation(outputName, out_relCols, copy.copy(inRel.storedWith))
    out_rel.update_columns()

    op = dag.Shuffle(out_rel, inputOpNode)
    # Add it as a child to input node
    inputOpNode.children.add(op)

    return op


def collect(inputOpNode, targetParty):

    # Get input relation from input node
    inRel = inputOpNode.out_rel
    inRel.storedWith = set([targetParty])


# Below functions are NOT part of the public API! Only used to simplify
# codegen testing

def _comp_neighs(inputOpNode, outputName, compColName):

    # Get input relation from input node
    inRel = inputOpNode.out_rel

    # Get relevant columns and create copies
    out_relCols = copy.deepcopy(inRel.columns)

    compCol = utils.find(inRel.columns, compColName)
    compCol.storedWith = set()

    for col in out_relCols:
        col.collSets = set()

    # Create output relation
    out_rel = rel.Relation(outputName, [copy.deepcopy(compCol)], copy.copy(inRel.storedWith))
    out_rel.update_columns()

    # Create our operator node
    op = dag.CompNeighs(out_rel, inputOpNode, compCol)

    # Add it as a child to input node
    inputOpNode.children.add(op)

    return op

def _index_join(leftInputNode, rightInputNode, outputName, leftColNames, rightColNames, indexOpNode):

    join_op = join(leftInputNode, rightInputNode,
                   outputName, leftColNames, rightColNames)
    idx_join_op = dag.IndexJoin.from_join(join_op, indexOpNode)

    leftInputNode.children.remove(join_op)
    rightInputNode.children.remove(join_op)

    leftInputNode.children.add(idx_join_op)
    rightInputNode.children.add(idx_join_op)
    indexOpNode.children.add(idx_join_op)

    return idx_join_op


def _persist(inputOpNode, outputName):

    out_rel = copy.deepcopy(inputOpNode.out_rel)
    out_rel.rename(outputName)
    persistOp = dag.Persist(out_rel, inputOpNode)
    inputOpNode.children.add(persistOp)
    return persistOp


def _close(inputOpNode, outputName, targetParties):

    out_rel = copy.deepcopy(inputOpNode.out_rel)
    out_rel.storedWith = targetParties
    out_rel.rename(outputName)
    closeOp = dag.Close(out_rel, inputOpNode)
    inputOpNode.children.add(closeOp)
    return closeOp


def _open(inputOpNode, outputName, targetParty):

    out_rel = copy.deepcopy(inputOpNode.out_rel)
    out_rel.storedWith = set([targetParty])
    out_rel.rename(outputName)
    openOp = dag.Open(out_rel, inputOpNode)
    inputOpNode.children.add(openOp)
    return openOp
