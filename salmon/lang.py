import copy
from salmon import rel
from salmon import dag
import salmon.utils as utils


def create(relName, columns, storedWith):

    columns = [rel.Column(relName, colName, idx, typeStr, collusionSet)
               for idx, (colName, typeStr, collusionSet) in enumerate(columns)]
    outRel = rel.Relation(relName, columns, storedWith)
    op = dag.Create(outRel)
    return op


def aggregate(inputOpNode, outputName, keyColName, overColName, aggregator, aggOutColName):

    # Get input relation from input node
    inRel = inputOpNode.outRel

    # Get relevant columns and reset their collusion sets
    inCols = inRel.columns
    keyCol = utils.find(inCols, keyColName)
    keyCol.collSets = set()
    overCol = utils.find(inCols, overColName)
    overCol.collSets = set()

    # Create output relation. Default column order is
    # key column first followed by column that will be
    # aggregated. Note that we want copies as these are
    # copies on the output relation and changes to them
    # shouldn't affect the original columns
    aggOutCol = copy.deepcopy(overCol)
    aggOutCol.name = aggOutColName
    outRelCols = [copy.deepcopy(keyCol), aggOutCol]
    outRel = rel.Relation(outputName, outRelCols, copy.copy(inRel.storedWith))
    outRel.updateColumns()

    # Create our operator node
    op = dag.Aggregate(outRel, inputOpNode, keyCol, overCol, aggregator)

    # Add it as a child to input node
    inputOpNode.children.add(op)

    return op


def project(inputOpNode, outputName, selectedColNames):

    # Get input relation from input node
    inRel = inputOpNode.outRel

    # Get relevant columns and create copies
    outRelCols = copy.deepcopy(inRel.columns)

    # Find all columns by name
    selectedCols = [utils.find(inRel.columns, colName)
                    for colName in selectedColNames]

    outRelCols = copy.deepcopy(selectedCols)
    for col in outRelCols:
        col.collSets = set()

    # Create output relation
    outRel = rel.Relation(outputName, outRelCols, copy.copy(inRel.storedWith))
    outRel.updateColumns()

    # Create our operator node
    op = dag.Project(outRel, inputOpNode, selectedCols)

    # Add it as a child to input node
    inputOpNode.children.add(op)

    return op


def divide(inputOpNode, outputName, targetColName, operands):

    # Get input relation from input node
    inRel = inputOpNode.outRel

    # Get relevant columns and create copies
    outRelCols = copy.deepcopy(inRel.columns)

    # Replace all column names with corresponding columns.
    operands = [utils.find(inRel.columns, op) if isinstance(
        op, str) else op for op in operands]
    for operand in operands:
        if hasattr(operand, "collSets"):
            operand.collSets = set()

    # if targetCol already exists, it will be at the 0th index of operands
    if targetColName == operands[0]:
        targetColumn = utils.find(inRel.columns, targetColName)
        targetColumn.collSets = set()
    else:
        # TODO: figure out new column's collSets
        targetColumn = rel.Column(
            outputName, targetColName, len(inputOpNode.columns), "INTEGER", set())
        outRelCols.append(targetColumn)

    # Create output relation
    outRel = rel.Relation(outputName, outRelCols, copy.copy(inRel.storedWith))
    outRel.updateColumns()

    # Create our operator node
    op = dag.Divide(outRel, inputOpNode, targetColumn, operands)

    # Add it as a child to input node
    inputOpNode.children.add(op)

    return op


def multiply(inputOpNode, outputName, targetColName, operands):

    # Get input relation from input node
    inRel = inputOpNode.outRel

    # Get relevant columns and create copies
    outRelCols = copy.deepcopy(inRel.columns)

    # Replace all column names with corresponding columns.
    operands = [utils.find(inRel.columns, op) if isinstance(
        op, str) else op for op in operands]
    for operand in operands:
        if hasattr(operand, "collSets"):
            operand.collSets = set()

    # if targetCol already exists, it will be at the 0th index of operands
    if targetColName == operands[0]:
        targetColumn = utils.find(inRel.columns, targetColName)
        targetColumn.collSets = set()
    else:
        # TODO: figure out new column's collSets
        targetColumn = rel.Column(
            outputName, targetColName, len(inputOpNode.columns), "INTEGER", set())
        outRelCols.append(targetColumn)

    # Create output relation
    outRel = rel.Relation(outputName, outRelCols, copy.copy(inRel.storedWith))
    outRel.updateColumns()

    # Create our operator node
    op = dag.Multiply(outRel, inputOpNode, targetColumn, operands)

    # Add it as a child to input node
    inputOpNode.children.add(op)

    return op


# TODO: is a self-join a problem?
def join(leftInputNode, rightInputNode, outputName, leftColName, rightColName):

    # TODO: technically this should take in a start index as well
    # This helper method takes in a relation, the key column of the join
    # and its index.
    # It returns a list of new columns with correctly merged collusion sets
    # for the output relation (in the same order as they appear on the input
    # relation but excluding the key column)
    def _colsFromRel(relation, keyCol, keyColIdx):

        resultCols = []
        for idx, col in enumerate(relation.columns):
            # Exclude key column
            if idx != keyColIdx:
                newCol = rel.Column(
                    outputName, col.getName(), idx, col.typeStr, set())
                resultCols.append(newCol)

        return resultCols

    # Get input relation from input nodes
    leftInRel = leftInputNode.outRel
    rightInRel = rightInputNode.outRel

    # Get columns from both relations
    leftCols = leftInRel.columns
    rightCols = rightInRel.columns

    # Get columns we will join on
    leftJoinCol = utils.find(leftCols, leftColName)
    rightJoinCol = utils.find(rightCols, rightColName)

    # # Get the key columns' merged collusion set
    # keyCollusionSet = utils.mergeCollusionSets(
    #     leftJoinCol.collusionSet, rightJoinCol.collusionSet)

    # Create new key column
    outKeyCol = rel.Column(
        outputName, leftJoinCol.getName(), 0, leftJoinCol.typeStr, set())

    # Define output relation columns.
    # These will be the key column followed
    # by all columns from left (other than join column)
    # and right (again excluding join column)
    outRelCols = [outKeyCol] \
        + _colsFromRel(leftInRel, outKeyCol, leftJoinCol.idx) \
        + _colsFromRel(rightInRel, outKeyCol, rightJoinCol.idx)

    # The result of the join will be stored with the union
    # of the parties storing left and right
    outStoredWith = leftInRel.storedWith.union(rightInRel.storedWith)

    # Create output relation
    outRel = rel.Relation(outputName, outRelCols, outStoredWith)
    outRel.updateColumns()

    # Create join operator
    op = dag.Join(
        outRel,
        leftInputNode,
        rightInputNode,
        leftJoinCol,
        rightJoinCol
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
    inRels = [inputOpNode.outRel for inputOpNode in inputOpNodes]

    # Ensure that all input relations have same
    # number of columns
    numCols = len(inRels[0].columns)
    for inRel in inRels:
        assert(len(inRel.columns) == numCols)
    if columnNames is not None:
        assert(len(columnNames) == numCols)

    # Copy over columns from existing relation
    outRelCols = copy.deepcopy(inRels[0].columns)
    for (i, col) in enumerate(outRelCols):
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
    outRel = rel.Relation(outputName, outRelCols, outStoredWith)
    outRel.updateColumns()

    # Create our operator node
    op = dag.Concat(outRel, inputOpNodes)

    # Add it as a child to each input node
    for inputOpNode in inputOpNodes:
        inputOpNode.children.add(op)

    return op


def collect(inputOpNode, targetParty):

    # Get input relation from input node
    inRel = inputOpNode.outRel
    inRel.storedWith = set([targetParty])


def _close(inputOpNode, outputName, targetParties):

    # Not part of the public API! Only used to simplify codegen testing
    outRel = copy.deepcopy(inputOpNode.outRel)
    outRel.storedWith = targetParties
    outRel.rename(outputName)
    closeOp = dag.Close(outRel, inputOpNode)
    inputOpNode.children.add(closeOp)
    return closeOp


def _open(inputOpNode, outputName, targetParty):

    # Not part of the public API! Only used to simplify codegen testing
    outRel = copy.deepcopy(inputOpNode.outRel)
    outRel.storedWith = set([targetParty])
    outRel.rename(outputName)
    openOp = dag.Open(outRel, inputOpNode)
    inputOpNode.children.add(openOp)
    return openOp
