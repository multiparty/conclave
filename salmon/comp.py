"""
Workflow graph optimizations and transformations.
"""
import copy
import warnings

import salmon.dag as saldag
import salmon.lang as sal
import salmon.utils as utils


def push_op_node_down(top_node, bottom_node):
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


def split_node(node):
    # Only dealing with single child case for now
    assert (len(node.children) <= 1)
    clone = copy.deepcopy(node)
    clone.out_rel.rename(node.out_rel.name + "_obl")
    clone.parents = set()
    clone.children = set()
    clone.is_mpc = True
    child = next(iter(node.children), None)
    saldag.insert_between(node, child, clone)


def fork_node(node):
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
        warnings.warn("hacky forkNode")
        clone.ordered = copy.copy(node.ordered)
        clone.children = set([child])
        for parent in clone.parents:
            parent.children.add(clone)
        node.children.remove(child)
        # make cloned node the child's new parent
        child.replace_parent(node, clone)
        child.update_op_specific_cols()


class DagRewriter:

    def __init__(self):

        # If true we visit topological ordering of dag in reverse
        self.reverse = False

    def rewrite(self, dag):

        ordered = dag.top_sort()
        if self.reverse:
            ordered = ordered[::-1]

        for node in ordered:
            print(type(self).__name__, "rewriting", node.out_rel.name)
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
            elif isinstance(node, saldag.Distinct):
                self._rewriteDistinct(node)
            else:
                msg = "Unknown class " + type(node).__name__
                raise Exception(msg)


class MPCPushDown(DagRewriter):

    def __init__(self):

        super(MPCPushDown, self).__init__()

    def _do_commute(self, top_op, bottom_op):

        # TODO: over-simplified
        # TODO: add rules for other ops
        if isinstance(top_op, saldag.Aggregate):
            if isinstance(bottom_op, saldag.Divide):
                return True
            else:
                return False
        else:
            return False

    def _rewriteDefault(self, node):

        node.is_mpc = node.requires_mpc()

    def _rewriteUnaryDefault(self, node):

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

    def _rewriteAggregate(self, node):

        parent = next(iter(node.parents))
        if parent.is_mpc:
            if isinstance(parent, saldag.Concat) and parent.is_boundary():
                splitNode(node)
                push_op_node_down(parent, node)
            else:
                node.is_mpc = True
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

        if node.requires_mpc():
            node.is_mpc = True
            if len(node.children) > 1 and node.is_boundary():
                forkNode(node)

    def _rewriteCreate(self, node):

        pass


class MPCPushUp(DagRewriter):

    def __init__(self):

        super(MPCPushUp, self).__init__()
        self.reverse = True

    def _rewriteUnaryDefault(self, node):

        par = next(iter(node.parents))
        if node.is_reversible() and node.is_lower_boundary() and not par.is_root():
            print("lower boundary", node)
            node.get_in_rel().stored_with = copy.copy(node.out_rel.stored_with)
            node.is_mpc = False

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

        pass

    def _rewriteConcat(self, node):

        # concats are always reversible so we just need to know
        # if we're dealing with a boundary node
        if node.is_lower_boundary():

            outStoredWith = node.out_rel.stored_with
            for par in node.parents:
                if not par.is_root():
                    par.out_rel.stored_with = copy.copy(outStoredWith)
            node.is_mpc = False

    def _rewriteCreate(self, node):

        pass


class CollSetPropDown(DagRewriter):

    def __init__(self):

        super(CollSetPropDown, self).__init__()

    def _rewriteAggregate(self, node):

        inGroupCols = node.group_cols
        outGroupCols = node.out_rel.columns[:-1]
        # TODO: (ben/malte) is the collSet propagation a 1:1 mapping here,
        # or is there a relationship between the collusion set associated
        # with two keyCols i & j?
        for i in range(len(outGroupCols)):
            outGroupCols[i].coll_sets |= copy.deepcopy(inGroupCols[i].coll_sets)
        inAggCol = node.agg_col
        outAggCol = node.out_rel.columns[-1]
        outAggCol.coll_sets |= copy.deepcopy(inAggCol.coll_sets)

    def _rewriteDivide(self, node):

        out_rel_cols = node.out_rel.columns
        operands = node.operands
        target_col = node.target_col

        # Update target column collusion set
        targetColOut = out_rel_cols[target_col.idx]

        targetColOut.coll_sets |= utils.coll_setsFromColumns(operands)

        # The other columns weren't modified so the collusion sets
        # simply carry over
        for inCol, outCol in zip(node.get_in_rel().columns, out_rel_cols):
            if inCol != target_col:
                outCol.coll_sets |= copy.deepcopy(inCol.coll_sets)

    def _rewriteProject(self, node):

        in_cols = node.get_in_rel().columns
        selected_cols = node.selected_cols

        for inCol, outCol in zip(selected_cols, node.out_rel.columns):
            outCol.coll_sets |= copy.deepcopy(inCol.coll_sets)

    def _rewriteFilter(self, node):

        in_cols = node.get_in_rel().columns
        out_rel_cols = node.out_rel.columns

        for inCol, outCol in zip(node.get_in_rel().columns, out_rel_cols):
            outCol.coll_sets |= copy.deepcopy(inCol.coll_sets)

    def _rewriteMultiply(self, node):

        out_rel_cols = node.out_rel.columns
        operands = node.operands
        target_col = node.target_col

        # Update target column collusion set
        targetColOut = out_rel_cols[target_col.idx]

        targetColOut.coll_sets |= utils.collSetsFromColumns(operands)

        # The other columns weren't modified so the collusion sets
        # simply carry over
        for inCol, outCol in zip(node.get_in_rel().columns, out_rel_cols):
            if inCol != target_col:
                outCol.coll_sets |= copy.deepcopy(inCol.coll_sets)

    def _rewriteHybridJoin(self, node):

        raise Exception("HybridJoin encountered during CollSetPropDown")

    def _rewriteJoin(self, node):

        leftInRel = node.get_left_in_rel()
        rightInRel = node.get_right_in_rel()

        left_join_cols = node.left_join_cols
        right_join_cols = node.right_join_cols

        numJoinCols = len(left_join_cols)

        outJoinCols = node.out_rel.columns[:numJoinCols]
        keyColsCollSets = []
        for i in range(len(left_join_cols)):
            keyColsCollSets.append(utils.mergeCollSets(
                left_join_cols[i].coll_sets, right_join_cols[i].coll_sets))
            outJoinCols[i].coll_sets = keyColsCollSets[i]

        absIdx = len(left_join_cols)
        for inCol in leftInRel.columns:
            if inCol not in set(left_join_cols):
                for keyColCollSets in keyColsCollSets:
                    node.out_rel.columns[absIdx].coll_sets = utils.mergeCollSets(
                        keyColCollSets, inCol.coll_sets)
                absIdx += 1

        for inCol in rightInRel.columns:
            if inCol not in set(right_join_cols):
                for keyColCollSets in keyColsCollSets:
                    node.out_rel.columns[absIdx].coll_sets = utils.mergeCollSets(
                        keyColCollSets, inCol.coll_sets)
                absIdx += 1

    def _rewriteConcat(self, node):

        # Copy over columns from existing relation
        out_rel_cols = node.out_rel.columns

        # Combine per-column collusion sets
        for idx, col in enumerate(out_rel_cols):
            columnsAtIdx = [in_rel.columns[idx] for in_rel in node.get_in_rels()]
            col.coll_sets = utils.coll_setsFromColumns(columnsAtIdx)

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

        if node.is_mpc:
            out_rel = node.out_rel
            keyColIdx = 0
            # oversimplifying here. what if there are multiple singleton
            # coll_sets?
            singletonCollSets = filter(
                lambda s: len(s) == 1,
                out_rel.columns[keyColIdx].coll_sets)
            singletonCollSets = sorted(list(singletonCollSets))
            if singletonCollSets:
                trusted_party = next(iter(singletonCollSets[0]))
                hybridJoinOp = saldag.HybridJoin.from_join(node, trusted_party)
                parents = hybridJoinOp.parents
                for par in parents:
                    par.replace_child(node, hybridJoinOp)

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
        # stored_with sets?
        warnings.warn("hacky insert store ops")
        inStoredWith = node.get_in_rel().stored_with
        outStoredWith = node.out_rel.stored_with
        if inStoredWith != outStoredWith:
            if (node.is_lower_boundary()):
                # input is stored with one set of parties
                # but output must be stored with another so we
                # need an open operation
                out_rel = copy.deepcopy(node.out_rel)
                out_rel.rename(out_rel.name + "_open")
                # reset stored_with on parent so input matches output
                node.out_rel.stored_with = copy.copy(inStoredWith)

                # create and insert store node
                storeOp = saldag.Open(out_rel, None)
                storeOp.is_mpc = True
                saldag.insert_between_children(node, storeOp)
            else:
                raise Exception(
                    "different stored_with on non-lower-boundary unary op", node)

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

    def _rewriteHybridJoin(self, node):

        self._rewriteJoin(node)

    def _rewriteJoin(self, node):

        outStoredWith = node.out_rel.stored_with
        orderedPars = [node.left_parent, node.right_parent]

        left_stored_with = node.get_left_in_rel().stored_with
        right_stored_with = node.get_right_in_rel().stored_with
        inStoredWith = left_stored_with | right_stored_with

        for parent in orderedPars:
            if (node.is_upper_boundary()):
                # Entering mpc mode so need to secret-share before op
                out_rel = copy.deepcopy(parent.out_rel)
                out_rel.rename(out_rel.name + "_close")
                out_rel.stored_with = copy.copy(inStoredWith)
                # create and insert close node
                closeOp = saldag.Close(out_rel, None)
                closeOp.is_mpc = True
                saldag.insert_between(parent, node, closeOp)
            # else:
            #     raise Exception(
            #         "different stored_with on non-upper-boundary join", node.debug_str())
        if node.is_leaf():
            if len(inStoredWith) > 1 and len(outStoredWith) == 1:
                targetParty = next(iter(outStoredWith))
                node.out_rel.stored_with = copy.copy(inStoredWith)
                sal._open(node, node.out_rel.name + "_open", targetParty)

    def _rewriteConcat(self, node):

        assert (not node.is_lower_boundary())

        outStoredWith = node.out_rel.stored_with
        orderedPars = node.get_sorted_parents()
        for parent in orderedPars:
            parStoredWith = parent.out_rel.stored_with
            if parStoredWith != outStoredWith:
                out_rel = copy.deepcopy(parent.out_rel)
                out_rel.rename(out_rel.name + "_close")
                out_rel.stored_with = copy.copy(outStoredWith)
                # create and insert close node
                storeOp = saldag.Close(out_rel, None)
                storeOp.is_mpc = True
                saldag.insert_between(parent, node, storeOp)

    def _rewriteCreate(self, node):

        pass


class ExpandCompositeOps(DagRewriter):
    """Replaces operator nodes that correspond to composite operations
    (for example hybrid joins) into subdags of primitive operators"""

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
        # TODO
        suffix = "rand"

        # in left parents' children, replace self with first primitive operator
        # in expanded subdag
        shuffledA = sal.shuffle(node.left_parent, "shuffledA")
        shuffledA.is_mpc = True
        node.left_parent.children.remove(node)

        # same for right parent
        shuffledB = sal.shuffle(node.right_parent, "shuffledB")
        shuffledB.is_mpc = True
        node.right_parent.children.remove(node)

        persistedB = sal._persist(shuffledB, "persistedB")
        persistedB.is_mpc = True
        persistedA = sal._persist(shuffledA, "persistedA")
        persistedA.is_mpc = True

        keysaclosed = sal.project(shuffledA, "keysaclosed", ["a"])
        keysaclosed.is_mpc = True
        keysbclosed = sal.project(shuffledB, "keysbclosed", ["c"])
        keysbclosed.is_mpc = True

        keysa = sal._open(keysaclosed, "keysa", 1)
        keysa.is_mpc = True
        keysb = sal._open(keysbclosed, "keysb", 1)
        keysb.is_mpc = True

        indexedA = sal.index(keysa, "indexedA", "indexA")
        indexedA.is_mpc = False

        indexedB = sal.index(keysb, "indexedB", "indexB")
        indexedB.is_mpc = False

        joinedindeces = sal.join(
            indexedA, indexedB, "joinedindeces", ["a"], ["c"])
        joinedindeces.is_mpc = False

        indecesonly = sal.project(
            joinedindeces, "indecesonly", ["indexA", "indexB"])
        indecesonly.is_mpc = False

        # TODO: update stored_with to use union of parent out_rel stored_with sets
        indecesclosed = sal._close(
            indecesonly, "indecesclosed", set([1, 2]))
        indecesclosed.is_mpc = True

        joined = sal._index_join(persistedA, persistedB, "joined", [
            "a"], ["c"], indecesclosed)
        joined.is_mpc = True

        # replace self with leaf of expanded subdag in each child node
        for child in node.get_sorted_children():
            child.replace_parent(node, joined)
        # add former children to children of leaf
        joined.children = node.children

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


def scotch(f):
    from salmon.codegen import scotch, CodeGenConfig

    def wrap():
        code = scotch.ScotchCodeGen(CodeGenConfig(), f())._generate(None, None)
        return code

    return wrap


def sharemind(f):
    from salmon.codegen import sharemind, CodeGenConfig

    # TODO: (ben) missing args to SharemindCodeGen, as
    # well as a call to with_sharemind_config. Also have
    # to figure out where to place with_sharemind_config
    # call (not here).
    def wrap():
        code = sharemind.SharemindCodeGen(
            CodeGenConfig(), f())._generate(None, None)
        return code

    return wrap


def mpc(*args):
    def _mpc(f):
        def wrapper(*args, **kwargs):
            dag = rewriteDag(saldag.OpDag(f()))
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
