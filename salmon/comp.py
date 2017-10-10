import copy
import salmon.utils as utils
import salmon.dag as saldag
import warnings


def pushOpNodeDown(topNode, bottomNode):

    # only dealing with one grandchild case for now
    assert(len(bottomNode.children) <= 1)
    child = next(iter(bottomNode.children), None)

    # remove bottom node between the bottom node's child
    # and the top node
    saldag.removeBetween(topNode, child, bottomNode)

    # we need all parents of the parent node
    grandParents = copy.copy(topNode.getSortedParents())

    # we will insert the removed bottom node between
    # each parent of the top node and the top node
    for idx, grandParent in enumerate(grandParents):
        toInsert = copy.deepcopy(bottomNode)
        toInsert.outRel.rename(toInsert.outRel.name + "_" + str(idx))
        toInsert.parents = set()
        toInsert.children = set()
        saldag.insertBetween(grandParent, topNode, toInsert)
        toInsert.updateStoredWith()


def splitNode(node):

    # Only dealing with single child case for now
    assert(len(node.children) <= 1)
    clone = copy.deepcopy(node)
    clone.outRel.rename(node.outRel.name + "_obl")
    clone.parents = set()
    clone.children = set()
    clone.isMPC = True
    child = next(iter(node.children), None)
    saldag.insertBetween(node, child, clone)


def forkNode(node):

    # we can skip the first child
    childIt = enumerate(copy.copy(node.getSortedChildren()))
    next(childIt)
    # clone node for each of the remaining children
    for idx, child in childIt:
        # create clone and rename output relation to
        # avoid identical relation names for different nodes
        clone = copy.deepcopy(node)
        clone.outRel.rename(node.outRel.name + "_" + str(idx))
        clone.parents = copy.copy(node.parents)
        warnings.warn("hacky forkNode")
        clone.ordered = copy.copy(node.ordered)
        clone.children = set([child])
        for parent in clone.parents:
            parent.children.add(clone)
        node.children.remove(child)
        # make cloned node the child's new parent
        child.replaceParent(node, clone)
        child.updateOpSpecificCols()


class DagRewriter:

    def __init__(self):

        # If true we visit topological ordering of dag in reverse
        self.reverse = False

    def rewrite(self, dag):

        ordered = dag.topSort()
        if self.reverse:
            ordered = ordered[::-1]

        for node in ordered:
            print(type(self).__name__, "rewriting", node.outRel.name)
            if isinstance(node, saldag.Aggregate):
                self._rewriteAggregate(node)
            elif isinstance(node, saldag.Divide):
                self._rewriteDivide(node)
            elif isinstance(node, saldag.Project):
                self._rewriteProject(node)
            elif isinstance(node, saldag.Filter):
                self._rewriteFilter(node)
            elif isinstance(node, saldag.Multiply):
                self._rewriteMultiply(node)
            elif isinstance(node, saldag.RevealJoin):
                self._rewriteRevealJoin(node)
            elif isinstance(node, saldag.HybridJoin):
                self._rewriteHybridJoin(node)
            elif isinstance(node, saldag.Join):
                self._rewriteJoin(node)
            elif isinstance(node, saldag.Concat):
                self._rewriteConcat(node)
            elif isinstance(node, saldag.Close):
                self._rewriteClose(node)
            elif isinstance(node, saldag.Open):
                self._rewriteOpen(node)
            elif isinstance(node, saldag.Create):
                self._rewriteCreate(node)
            else:
                msg = "Unknown class " + type(node).__name__
                raise Exception(msg)


class MPCPushDown(DagRewriter):

    def __init__(self):

        super(MPCPushDown, self).__init__()

    def _rewriteDefault(self, node):

        node.isMPC = node.requiresMPC()

    def _rewriteUnaryDefault(self, node):

        parent = next(iter(node.parents))
        if parent.isMPC:
            # if we have an MPC parent we can try and pull it down
            # the leaf condition is to avoid issues with storedWith
            # getting overwritten
            if isinstance(parent, saldag.Concat) and parent.isBoundary() and not node.isLeaf():
                pushOpNodeDown(parent, node)
            else:
                node.isMPC = True
        else:
            pass

    def _rewriteAggregate(self, node):

        parent = next(iter(node.parents))
        if parent.isMPC:
            if isinstance(parent, saldag.Concat) and parent.isBoundary():
                splitNode(node)
                pushOpNodeDown(parent, node)
            else:
                node.isMPC = True
        else:
            pass

    def _rewriteProject(self, node):

        self._rewriteUnaryDefault(node)

    def _rewriteFilter(self, node):

        self._rewriteUnaryDefault(node)

    def _rewriteMultiply(self, node):

        self._rewriteUnaryDefault(node)

    def _rewriteDivide(self, node):

        self._rewriteUnaryDefault(node)

    def _rewriteRevealJoin(self, node):

        raise Exception("RevealJoin encountered during MPCPushDown")

    def _rewriteHybridJoin(self, node):

        raise Exception("HybridJoin encountered during MPCPushDown")

    def _rewriteJoin(self, node):

        self._rewriteDefault(node)

    def _rewriteConcat(self, node):

        if node.requiresMPC():
            node.isMPC = True
            if len(node.children) > 1 and node.isBoundary():
                forkNode(node)

    def _rewriteCreate(self, node):

        pass


class MPCPushUp(DagRewriter):

    def __init__(self):

        super(MPCPushUp, self).__init__()
        self.reverse = True

    def _rewriteUnaryDefault(self, node):

        par = next(iter(node.parents))
        if node.isReversible() and node.isLowerBoundary() and not par.isRoot():
            node.getInRel().storedWith = copy.copy(node.outRel.storedWith)
            node.isMPC = False

    def _rewriteAggregate(self, node):

        pass

    def _rewriteDivide(self, node):

        self._rewriteUnaryDefault(node)

    def _rewriteProject(self, node):

        self._rewriteUnaryDefault(node)

    def _rewriteFilter(self, node):

        self._rewriteUnaryDefault(node)

    def _rewriteMultiply(self, node):

        self._rewriteUnaryDefault(node)

    def _rewriteRevealJoin(self, node):

        raise Exception("RevealJoin encountered during MPCPushUp")

    def _rewriteHybridJoin(self, node):

        raise Exception("HybridJoin encountered during MPCPushUp")

    def _rewriteJoin(self, node):

        if node.isLowerBoundary():
            leftStoredWith = node.getLeftInRel().storedWith
            rightStoredWith = node.getRightInRel().storedWith
            outStoredWith = node.outRel.storedWith

            revealJoinOp = None
            if outStoredWith == leftStoredWith:
                # sanity check
                assert outStoredWith != rightStoredWith
                revealJoinOp = saldag.RevealJoin.fromJoin(
                    node, node.getLeftInRel(), outStoredWith)
            elif outStoredWith == rightStoredWith:
                revealJoinOp = saldag.RevealJoin.fromJoin(
                    node, node.getLeftInRel(), outStoredWith)

            # TODO: update storedWith on revealJoin
            if revealJoinOp:
                parents = revealJoinOp.parents
                for par in parents:
                    par.replaceChild(node, revealJoinOp)

    def _rewriteConcat(self, node):

        # concats are always reversible so we just need to know
        # if we're dealing with a boundary node
        if node.isLowerBoundary():

            outStoredWith = node.outRel.storedWith
            for par in node.parents:
                if not par.isRoot():
                    par.outRel.storedWith = copy.copy(outStoredWith)
            node.isMPC = False

    def _rewriteCreate(self, node):

        pass


class CollSetPropDown(DagRewriter):

    def __init__(self):

        super(CollSetPropDown, self).__init__()

    def _rewriteAggregate(self, node):

        inGroupCols = node.groupCols
        outGroupCols = node.outRel.columns[:-1]
        # TODO: (ben/malte) is the collSet propagation a 1:1 mapping here,
        # or is there a relationship between the collusion set associated
        # with two keyCols i & j?
        for i in range(len(outGroupCols)):
            outGroupCols[i].collSets |= copy.deepcopy(inGroupCols[i].collSets)
        inAggCol = node.aggCol
        outAggCol = node.outRel.columns[-1]
        outAggCol.collSets |= copy.deepcopy(inAggCol.collSets)

    def _rewriteDivide(self, node):

        outRelCols = node.outRel.columns
        operands = node.operands
        targetCol = node.targetCol

        # Update target column collusion set
        targetColOut = outRelCols[targetCol.idx]

        targetColOut.collSets |= utils.collSetsFromColumns(operands)

        # The other columns weren't modified so the collusion sets
        # simply carry over
        for inCol, outCol in zip(node.getInRel().columns, outRelCols):
            if inCol != targetCol:
                outCol.collSets |= copy.deepcopy(inCol.collSets)

    def _rewriteProject(self, node):

        inCols = node.getInRel().columns
        selectedCols = node.selectedCols

        for inCol, outCol in zip(selectedCols, node.outRel.columns):
            outCol.collSets |= copy.deepcopy(inCol.collSets)

    def _rewriteFilter(self, node):

        inCols = node.getInRel().columns
        outRelCols = node.outRel.columns

        for inCol, outCol in zip(node.getInRel().columns, outRelCols):
            outCol.collSets |= copy.deepcopy(inCol.collSets)

    def _rewriteMultiply(self, node):

        outRelCols = node.outRel.columns
        operands = node.operands
        targetCol = node.targetCol

        # Update target column collusion set
        targetColOut = outRelCols[targetCol.idx]

        targetColOut.collSets |= utils.collSetsFromColumns(operands)

        # The other columns weren't modified so the collusion sets
        # simply carry over
        for inCol, outCol in zip(node.getInRel().columns, outRelCols):
            if inCol != targetCol:
                outCol.collSets |= copy.deepcopy(inCol.collSets)

    def _rewriteRevealJoin(self, node):

        self._rewriteJoin(node)

    def _rewriteHybridJoin(self, node):

        raise Exception("HybridJoin encountered during CollSetPropDown")

    def _rewriteJoin(self, node):

        leftInRel = node.getLeftInRel()
        rightInRel = node.getRightInRel()

        leftJoinCols = node.leftJoinCols
        rightJoinCols = node.rightJoinCols

        numJoinCols = len(leftJoinCols)

        outJoinCols = node.outRel.columns[:numJoinCols]
        keyColsCollSets = []
        for i in range(len(leftJoinCols)):
            keyColsCollSets.append(utils.mergeCollSets(
                leftJoinCols[i].collSets, rightJoinCols[i].collSets))
            outJoinCols[i].collSets = keyColsCollSets[i]

        absIdx = len(leftJoinCols)
        for inCol in leftInRel.columns:
            if inCol not in set(leftJoinCols):
                for keyColCollSets in keyColsCollSets:
                    node.outRel.columns[absIdx].collSets = utils.mergeCollSets(
                        keyColCollSets, inCol.collSets)
                absIdx += 1

        for inCol in rightInRel.columns:
            if inCol not in set(rightJoinCols):
                for keyColCollSets in keyColsCollSets:
                    node.outRel.columns[absIdx].collSets = utils.mergeCollSets(
                        keyColCollSets, inCol.collSets)
                absIdx += 1

    def _rewriteConcat(self, node):

        # Copy over columns from existing relation
        outRelCols = node.outRel.columns

        # Combine per-column collusion sets
        for idx, col in enumerate(outRelCols):
            columnsAtIdx = [inRel.columns[idx] for inRel in node.getInRels()]
            col.collSets = utils.collSetsFromColumns(columnsAtIdx)

    def _rewriteCreate(self, node):

        pass


class CollSetPropUp(DagRewriter):

    def __init__(self):

        super(CollSetPropUp, self).__init__()
        self.reverse = True

    def _rewriteAggregate(self, node):

        pass

    def _rewriteDivide(self, node):

        pass

    def _rewriteProject(self, node):

        pass

    def _rewriteMultiply(self, node):

        pass

    def _rewriteRevealJoin(self, node):

        pass

    def _rewriteHybridJoin(self, node):

        pass

    def _rewriteJoin(self, node):

        pass

    def _rewriteConcat(self, node):

        pass

    def _rewriteCreate(self, node):

        pass


class HybridJoinOpt(DagRewriter):

    def __init__(self):

        super(HybridJoinOpt, self).__init__()

    def _rewriteAggregate(self, node):

        pass

    def _rewriteProject(self, node):

        pass

    def _rewriteFilter(self, node):

        pass

    def _rewriteDivide(self, node):

        pass

    def _rewriteMultiply(self, node):

        pass

    def _rewriteRevealJoin(self, node):

        # TODO
        pass

    def _rewriteHybridJoin(self, node):

        raise Exception("HybridJoin encountered during HybridJoinOpt")

    def _rewriteJoin(self, node):

        if node.isMPC:
            outRel = node.outRel
            keyColIdx = 0
            # oversimplifying here. what if there are multiple singleton
            # collSets?
            singletonCollSets = filter(
                lambda s: len(s) == 1,
                outRel.columns[keyColIdx].collSets)
            singletonCollSets = sorted(list(singletonCollSets))
            if singletonCollSets:
                trustedParty = next(iter(singletonCollSets[0]))
                hybridJoinOp = saldag.HybridJoin.fromJoin(node, trustedParty)
                parents = hybridJoinOp.parents
                for par in parents:
                    par.replaceChild(node, hybridJoinOp)

    def _rewriteConcat(self, node):

        pass

    def _rewriteCreate(self, node):

        pass


class InsertOpenAndCloseOps(DagRewriter):
    # TODO: this class is messy

    def __init__(self):

        super(InsertOpenAndCloseOps, self).__init__()

    def _rewriteDefaultUnary(self, node):

        # TODO: can there be a case when children have different
        # storedWith sets?
        warnings.warn("hacky insert store ops")
        inStoredWith = node.getInRel().storedWith
        outStoredWith = node.outRel.storedWith
        if inStoredWith != outStoredWith:
            if (node.isLowerBoundary()):
                # input is stored with one set of parties
                # but output must be stored with another so we
                # need a store operation
                outRel = copy.deepcopy(node.outRel)
                outRel.rename(outRel.name + "_open")
                # reset storedWith on parent so input matches output
                node.outRel.storedWith = copy.copy(inStoredWith)

                # create and insert store node
                storeOp = saldag.Open(outRel, None)
                storeOp.isMPC = True
                saldag.insertBetweenChildren(node, storeOp)
            else:
                raise Exception(
                    "different storedWith on non-lower-boundary unary op", node)

    def _rewriteAggregate(self, node):

        self._rewriteDefaultUnary(node)

    def _rewriteDivide(self, node):

        self._rewriteDefaultUnary(node)

    def _rewriteProject(self, node):

        self._rewriteDefaultUnary(node)

    def _rewriteFilter(self, node):

        self._rewriteDefaultUnary(node)

    def _rewriteMultiply(self, node):

        self._rewriteDefaultUnary(node)

    def _rewriteRevealJoin(self, node):

        # TODO: should use storedWith for this, not upperBoundary
        if node.isUpperBoundary():
            # need to secret-share before reveal join in
            # upper boundary case
            leftStoredWith = node.getLeftInRel().storedWith
            rightStoredWith = node.getRightInRel().storedWith
            combinedStoreWith = leftStoredWith | rightStoredWith
            orderedPars = node.getSortedParents()
            for parent in orderedPars:
                outRel = copy.deepcopy(parent.outRel)
                outRel.rename(outRel.name + "_close")
                outRel.storedWith = copy.copy(combinedStoreWith)
                # create and insert store node
                storeOp = saldag.Close(outRel, None)
                storeOp.isMPC = True
                saldag.insertBetween(parent, node, storeOp)

    def _rewriteHybridJoin(self, node):

        self._rewriteJoin(node)

    def _rewriteJoin(self, node):

        outStoredWith = node.outRel.storedWith
        orderedPars = [node.leftParent, node.rightParent]
        for parent in orderedPars:
            parStoredWith = parent.outRel.storedWith
            if parStoredWith != outStoredWith:
                if (node.isUpperBoundary()):
                    # Entering mpc mode so need to secret-share before op
                    outRel = copy.deepcopy(parent.outRel)
                    outRel.rename(outRel.name + "_close")
                    outRel.storedWith = copy.copy(outStoredWith)
                    # create and insert store node
                    storeOp = saldag.Close(outRel, None)
                    storeOp.isMPC = True
                    saldag.insertBetween(parent, node, storeOp)
                else:
                    raise Exception(
                        "different storedWith on non-upper-boundary join", node.dbgStr())

    def _rewriteConcat(self, node):

        assert(not node.isLowerBoundary())

        outStoredWith = node.outRel.storedWith
        orderedPars = node.getSortedParents()
        for parent in orderedPars:
            parStoredWith = parent.outRel.storedWith
            if parStoredWith != outStoredWith:
                outRel = copy.deepcopy(parent.outRel)
                outRel.rename(outRel.name + "_close")
                outRel.storedWith = copy.copy(outStoredWith)
                # create and insert close node
                storeOp = saldag.Close(outRel, None)
                storeOp.isMPC = True
                saldag.insertBetween(parent, node, storeOp)

    def _rewriteCreate(self, node):

        pass


class ExpandCompositeOps(DagRewriter):
    """Replaces operator nodes that correspond to composite operations
    for example hybrid joins into subdags of primitive operators"""

    def __init__(self):

        super(ExpandCompositeOps, self).__init__()

    def _rewriteAggregate(self, node):

        pass

    def _rewriteDivide(self, node):

        pass

    def _rewriteProject(self, node):

        pass

    def _rewriteFilter(self, node):

        pass

    def _rewriteMultiply(self, node):

        pass

    def _rewriteRevealJoin(self, node):

        pass

    def _rewriteHybridJoin(self, node):

        pass

    def _rewriteJoin(self, node):

        pass

    def _rewriteConcat(self, node):

        pass

    def _rewriteCreate(self, node):

        pass

    def _rewriteOpen(self, node):

        pass

    def _rewriteClose(self, node):

        pass


def rewriteDag(dag):

    MPCPushDown().rewrite(dag)
    # ironic?
    MPCPushUp().rewrite(dag)
    CollSetPropDown().rewrite(dag)
    HybridJoinOpt().rewrite(dag)
    InsertOpenAndCloseOps().rewrite(dag)
    ExpandCompositeOps().rewrite(dag)
    return dag


def pruneDag(dag, party):

    # given party and dag, remove all op nodes from dag that party
    # is not involved in
    ordered = dag.topSort()

    for node in ordered:
        parents = node.parents
        inputStoredWith = set().union(
            *[par.outRel.storedWith for par in parents])
        inInput = party in inputStoredWith
        inOutput = party in node.outRel.storedWith
        if not (inInput or inOutput):
            parents = node.parents
            children = node.children
            if node.isRoot():                
                dag.roots.remove(node)
                dag.roots |= children
                for child in children:
                    child.parents.remove(node)
            elif node.isLeaf():
                for parent in parents:
                    parent.children.remove(node)
            else:
                if (len(parents) > 1 or len(children) > 1):
                    raise NotImplementedError()
                else:
                    parent = next(iter(parents))
                    child = next(iter(children))
                    saldag.removeBetween(parent, child, node)
    for node in dag.topSort():
        if node.isRoot() and not isinstance(node, saldag.Create):
            # only handle unary nodes for now
            if not isinstance(node, saldag.UnaryOpNode):
                raise NotImplementedError()
            assert node.parent
            # we cheated, so "orphan" nodes still have parents
            create_op = saldag.Create(node.getInRel())
            # create op is in same mode as node
            create_op.isMPC = node.isMPC
            create_op.children.add(node)
            node.parent = create_op
            node.parents = {create_op}
            dag.roots.remove(node)
            dag.roots.add(create_op)
    return dag


def scotch(f):

    from salmon.codegen import scotch

    def wrap():
        code = scotch.ScotchCodeGen(f())._generate(None, None)
        return code

    return wrap


def sharemind(f):

    from salmon.codegen import sharemind

    def wrap():
        code = sharemind.SharemindCodeGen(f())._generate(None, None)
        return code

    return wrap


def mpc(*args):
    def _mpc(f):
        def wrapper(*args, **kwargs):
            dag = rewriteDag(saldag.OpDag(f()))
            if party:
                dag = pruneDag(dag, party)
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


def dagonly(f):

    def wrap():
        return saldag.OpDag(f())
    return wrap
