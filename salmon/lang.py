import copy
from salmon import rel
from salmon import dag
import salmon.utils as utils

def create(name, columns):

    columns = [rel.Column(name, idx, typeStr, collusionSet) 
        for idx, (typeStr, collusionSet) in enumerate(columns)]
    outRel = rel.Relation(name, columns)
    op = dag.Create(outRel)
    return op

def aggregate(inputOpNode, outputName, keyColName, aggColName, aggregator):

    # Get input relation from input node
    inRel = inputOpNode.outRel

    # Get relevant columns
    inCols = inRel.columns
    keyCol = utils.find(inCols, keyColName)
    aggCol = utils.find(inCols, aggColName)
    
    # Create output relation. Default column order is
    # key column first followed by column that will be 
    # aggregated. Note that we want copies as these are
    # copies on the output relation and changes to them
    # shouldn't affect the original columns
    outRelCols = [copy.deepcopy(keyCol), copy.deepcopy(aggCol)]
    outRel = rel.Relation(outputName, outRelCols)
    outRel.updateColumns()

    # Create our operator node
    op = dag.Aggregate(outRel, inputOpNode, keyCol, aggCol, aggregator)
    
    # Add it as a child to input node 
    inputOpNode.children.add(op)

    return op

def project(inputOpNode, outputName, selectedColNames):

    # Get input relation from input node
    inRel = inputOpNode.outRel

    # Get relevant columns and create copies
    outRelCols = copy.deepcopy(inRel.columns)

    # Find all columns by name
    selectedCols = [utils.find(inRel.columns, colName) for colName in selectedColNames]
    outRelCols = copy.deepcopy(selectedCols)

    # Create output relation
    outRel = rel.Relation(outputName, outRelCols)
    outRel.updateColumns()

    # Create our operator node
    op = dag.Project(outRel, inputOpNode, selectedCols)
    
    # Add it as a child to input node 
    inputOpNode.children.add(op)

    return op

def multiply(inputOpNode, outputName, targetColName, operands):

    # Get input relation from input node
    inRel = inputOpNode.outRel

    # Get relevant columns and create copies
    outRelCols = copy.deepcopy(inRel.columns)
    
    # Create result column. By default we add it to the
    # output relation as the first column
    # Its collusion set is the union of all operand collusion
    # sets

    # Replace all column names with corresponding columns.
    operands = [utils.find(outRelCols, op) if isinstance(op, str) else op for op in operands]
    operands = copy.deepcopy(operands)

    # Update target column collusion set
    targetCollusionSet = utils.collusionSetUnion(operands)
    targetColumn = utils.find(outRelCols, targetColName)
    targetColumn.collusionSet = targetCollusionSet
    
    # Create output relation
    outRel = rel.Relation(outputName, outRelCols)
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
                # This is somewhat nuanced. The collusion set
                # of col knows the values of the result but not
                # the linkage of these values to the key column values.
                # Thus we must take the union of the collusion set of
                # col *and* the collusion set of the key column for the
                # new column.
                newColSet = utils.mergeCollusionSets(
                    col.collusionSet, keyCol.collusionSet)

                newCol = rel.Column(
                    outputName, idx, col.typeStr, newColSet)
                
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

    # Get the key columns' merged collusion set
    keyCollusionSet = utils.mergeCollusionSets(
        leftJoinCol.collusionSet, rightJoinCol.collusionSet)

    # Create new key column
    outKeyCol = rel.Column(
        outputName, 0, leftJoinCol.typeStr, keyCollusionSet)

    # Define output relation columns.
    # These will be the key column followed
    # by all columns from left (other than join column)
    # and right (again excluding join column)
    outRelCols = [outKeyCol] \
               + _colsFromRel(leftInRel, outKeyCol, leftJoinCol.idx) \
               + _colsFromRel(rightInRel, outKeyCol, rightJoinCol.idx)

    # Create output relation
    outRel = rel.Relation(outputName, outRelCols)
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

def concat(inputOpNodes, outputName):

    # Make sure we have at least two input node as a
    # sanity check--could relax this in the future
    assert(len(inputOpNodes) >= 2)

    # Get input relations from input nodes
    inRels = [inputOpNode.outRel for inputOpNode in inputOpNodes]

    # Ensure that all input relations have same
    # number of columns
    relLens = [len(inRel.columns) for inRel in inRels]
    relSizesEqual = len(set(relLens)) == 1
    assert(relSizesEqual)

    # Copy over columns from existing relation 
    outRelCols = copy.deepcopy(inRels[0].columns)
    
    # Combine per-column collusion sets
    for idx, col in enumerate(outRelCols):
        columnsAtIndex = [inRel.columns[idx] for inRel in inRels]
        col.collusionSet = utils.collusionSetUnion(columnsAtIndex)

    # Create output relation
    outRel = rel.Relation(outputName, outRelCols)
    outRel.updateColumns()

    # Create our operator node
    op = dag.Concat(outRel, inputOpNodes)
    
    # Add it as a child to each input node 
    for inputOpNode in inputOpNodes:
        inputOpNode.children.add(op)

    return op

def collect(inputOpNode, outputName, targetParty):

    # Get input relation from input node
    inRel = inputOpNode.outRel

    # Get relevant columns and create copies
    outRelCols = copy.deepcopy(inRel.columns)

    # Update collusion sets
    for outRelCol in outRelCols:
        outRelCol.updateCollSetWith(set([targetParty]))

    # Create output relation
    outRel = rel.Relation(outputName, outRelCols)
    outRel.updateColumns()

    # Create our operator node
    op = dag.Store(outRel, inputOpNode)
    
    # Add it as a child to input node 
    inputOpNode.children.add(op)

    return op
