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
    op = dag.Aggregate(inRel, outRel, keyCol, aggCol, aggregator)
    
    # Add it as a child to input node 
    inputOpNode.addChild(op)

    return op

def project(inputOpNode, outputName, projector):

    # Get input relation from input node
    inRel = inputOpNode.outRel

    # Get relevant columns and create copies
    outRelCols = copy.deepcopy(inRel.columns)
    
    # Create output relation
    outRel = rel.Relation(outputName, outRelCols)
    outRel.updateColumns()

    # Create our operator node
    op = dag.Project(inRel, outRel, projector)
    
    # Add it as a child to input node 
    inputOpNode.addChild(op)

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
    # Constants will be replaced with empty sets 
    # (indicating an empty collusion set for the next step)
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
    op = Multiply(inRel, outRel, targetColumn, operands)
    
    # Add it as a child to input node 
    inputOpNode.addChild(op)

    return op

# TODO: is a self-join a problem?
def join(leftInputNode, rightInputNode, outputName, leftColIdx, rightColIdx):

    # TODO: technically this should take in a start index as well
    # This helper method takes in a relation, the key column of the join 
    # and its index. 
    # It returns a list of new columns with correctly merged collusion sets
    # for the output relation (in the same order as they appear on the input
    # relation but excluding the key column)
    def _colsFromRel(rel, keyCol, keyColIdx):

        resultCols = []
        for idx, col in enumerate(rel.columns):
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
    leftJoinCol = leftCols[leftColIdx]
    rightJoinCol = rightCols[rightColIdx]

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
               + _colsFromRel(leftInRel, outKeyCol, leftColIdx) \
               + _colsFromRel(rightInRel, outKeyCol, rightColIdx)

    # Create output relation
    outRel = rel.Relation(outputName, outRelCols)
    outRel.updateColumns()

    # Create join operator
    op = Join(
        leftInRel, 
        rightInRel, 
        outRel, 
        leftJoinCol, 
        rightJoinCol
    )

    # Add it as a child to both input nodes 
    leftInputNode.addChild(op)
    rightInputNode.addChild(op)

    return op
