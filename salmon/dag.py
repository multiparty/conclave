"""
Data structure for representing a workflow directed acyclic graph (DAG).
"""
import copy
from salmon import rel


class Node:
    """
    Graph node data structure.
    """

    def __init__(self, name: str):
        """Initalize graph node object."""
        self.name = name
        self.children = set()
        self.parents = set()

    def debug_str(self):
        """Return extended string representation for debugging."""
        children_str = str([n.name for n in self.children])
        parent_str = str([n.name for n in self.parents])
        return self.name + " children: " + children_str + " parents: " + parent_str

    def is_leaf(self):
        """Return whether node is a leaf."""
        return len(self.children) == 0

    def is_root(self):
        """Return whether node is a root."""
        return len(self.parents) == 0

    def __str__(self):
        """Return string representation of node."""
        return self.name


class OpNode(Node):

    def __init__(self, name: str, out_rel: rel.Relation):

        super(OpNode, self).__init__(name)
        self.out_rel = out_rel
        # By default we assume that the operator requires data
        # to cross party boundaries. Override this for operators
        # where this is not the case
        self.is_local = False
        self.is_mpc = False

    # Indicates whether a node is at the boundary of MPC
    # i.e. if nodes above it are local (there are operators
    # such as aggregations that override this method since
    # other rules apply there)
    def is_boundary(self):
        # TODO: could this be (self.is_upper_boundary() or
        # self.is_lower_boundary())?
        return self.is_upper_boundary()

    def is_upper_boundary(self):

        return self.is_mpc and not any([par.is_mpc and not isinstance(par, Close) for par in self.parents])

    def is_lower_boundary(self):

        return self.is_mpc and not any([child.is_mpc and not isinstance(child, Open) for child in self.children])

    # By default operations are not reversible, i.e., given
    # the output of the operation we cannot learn the input
    # Note: for now we are only considering whether an entire relation
    # is reversible as opposed to column level reversibility
    def is_reversible(self):

        return False

    def requires_mpc(self):

        return True

    def update_op_specific_cols(self):

        return

    def update_stored_with(self):

        return

    def make_orphan(self):

        # TODO: set self.parent = None?
        self.parents = set()

    def remove_parent(self, parent: Node):

        self.parents.remove(parent)

    def replace_parent(self, old_parent: Node, new_parent: Node):

        self.parents.remove(old_parent)
        self.parents.add(new_parent)

    def replace_child(self, old_child: Node, new_child: Node):

        self.children.remove(old_child)
        self.children.add(new_child)

    def get_sorted_children(self):

        return sorted(list(self.children), key=lambda x: x.out_rel.name)

    def get_sorted_parents(self):

        return sorted(list(self.parents), key=lambda x: x.out_rel.name)

    def __str__(self):

        return "{}{}->{}".format(
            super(OpNode, self).__str__(),
            "mpc" if self.is_mpc else "",
            self.out_rel.name
        )


class UnaryOpNode(OpNode):

    def __init__(self, name: str, out_rel: rel.Relation, parent: OpNode):

        super(UnaryOpNode, self).__init__(name, out_rel)
        self.parent = parent
        if self.parent:
            self.parents.add(parent)

    def get_in_rel(self):

        return self.parent.out_rel

    def requires_mpc(self):

        return self.get_in_rel().is_shared() and not self.is_local

    def update_stored_with(self):

        self.out_rel.stored_with = copy.copy(self.get_in_rel().stored_with)

    def make_orphan(self):

        super(UnaryOpNode, self).make_orphan()
        self.parent = None

    def replace_parent(self, old_parent, new_parent):

        super(UnaryOpNode, self).replace_parent(old_parent, new_parent)
        self.parent = new_parent

    def remove_parent(self, parent):

        super(UnaryOpNode, self).remove_parent(parent)
        self.parent = None


class BinaryOpNode(OpNode):

    def __init__(self, name: str, out_rel: rel.Relation, left_parent: OpNode, right_parent: OpNode):

        super(BinaryOpNode, self).__init__(name, out_rel)
        self.left_parent = left_parent
        self.right_parent = right_parent
        if self.left_parent:
            self.parents.add(left_parent)
        if self.right_parent:
            self.parents.add(right_parent)

    def get_left_in_rel(self):

        return self.left_parent.out_rel

    def get_right_in_rel(self):

        return self.right_parent.out_rel

    def requires_mpc(self):

        left_stored_with = self.get_left_in_rel().stored_with
        right_stored_with = self.get_right_in_rel().stored_with
        combined = left_stored_with.union(right_stored_with)
        return (len(combined) > 1) and not self.is_local

    def make_orphan(self):

        super(UnaryOpNode, self).make_orphan()
        self.left_parent = None
        self.right_parent = None

    def replace_parent(self, old_parent: Node, new_parent: OpNode):

        super(BinaryOpNode, self).replace_parent(old_parent, new_parent)
        if self.left_parent == old_parent:
            self.left_parent = new_parent
        elif self.right_parent == old_parent:
            self.right_parent = new_parent

    def remove_parent(self, parent: OpNode):

        super(BinaryOpNode, self).remove_parent(parent)
        if self.left_parent == parent:
            self.left_parent = None
        elif self.right_parent == parent:
            self.right_parent = None


class NaryOpNode(OpNode):

    def __init__(self, name: str, out_rel: rel.Relation, parents: set):

        super(NaryOpNode, self).__init__(name, out_rel)
        self.parents = parents

    def get_in_rels(self):

        # Returning a set here to emphasize that the order of
        # the returned relations is meaningless (since the parent-set
        # where we're getting the relations from isn't ordered).
        # If we want operators with multiple input relations where
        # the order matters, we do implement it as a separate class.
        return set([parent.out_rel for parent in self.parents])

    def requires_mpc(self):

        in_coll_sets = [in_rel.stored_with for in_rel in self.get_in_rels()]
        in_rels_shared = len(set().union(*in_coll_sets)) > 1
        return in_rels_shared and not self.is_local


class Create(UnaryOpNode):

    def __init__(self, out_rel: rel.Relation):

        super(Create, self).__init__("create", out_rel, None)
        # Input can be done by parties locally
        self.is_local = True

    def requires_mpc(self):

        return False


class Store(UnaryOpNode):

    def __init__(self, out_rel: rel.Relation, parent: OpNode):

        super(Store, self).__init__("store", out_rel, parent)

    def is_reversible(self):

        return True


class Persist(UnaryOpNode):

    def __init__(self, out_rel: rel.Relation, parent: OpNode):

        super(Persist, self).__init__("persist", out_rel, parent)

    def is_reversible(self):

        return True


class Open(UnaryOpNode):

    def __init__(self, out_rel: rel.Relation, parent: OpNode):

        super(Open, self).__init__("open", out_rel, parent)
        self.is_mpc = True

    def is_reversible(self):

        return True


class Close(UnaryOpNode):

    def __init__(self, out_rel: rel.Relation, parent: OpNode):

        super(Close, self).__init__("close", out_rel, parent)
        self.is_mpc = True

    def is_reversible(self):

        return True


class Send(UnaryOpNode):

    def __init__(self, out_rel: rel.Relation, parent: OpNode):

        super(Send, self).__init__("send", out_rel, parent)

    def is_reversible(self):

        return True


class Concat(NaryOpNode):

    def __init__(self, out_rel: rel.Relation, parents: list):

        parent_set = set(parents)
        # sanity check for now
        assert(len(parents) == len(parent_set))
        super(Concat, self).__init__("concat", out_rel, parent_set)
        self.ordered = parents

    def is_reversible(self):

        return True

    def get_in_rels(self):

        return [parent.out_rel for parent in self.ordered]

    def replace_parent(self, old_parent: OpNode, new_parent: OpNode):

        super(Concat, self).replace_parent(old_parent, new_parent)
        # this will throw if old_parent not in list
        idx = self.ordered.index(old_parent)
        self.ordered[idx] = new_parent

    def remove_parent(self, parent: OpNode):

        raise NotImplementedError()


class Aggregate(UnaryOpNode):

    def __init__(self, out_rel: rel.Relation, parent: OpNode,
                 group_cols: list, agg_col: rel.Column, aggregator: str):

        super(Aggregate, self).__init__("aggregation", out_rel, parent)
        self.group_cols = group_cols
        self.agg_col = agg_col
        self.aggregator = aggregator

    def update_op_specific_cols(self):

        # TODO: do we need to copy here?
        self.group_cols = [self.get_in_rel().columns[group_col.idx]
                           for group_col in self.group_cols]
        self.agg_col = self.get_in_rel().columns[self.agg_col.idx]


class IndexAggregate(Aggregate):

    def __init__(self, out_rel: rel.Relation, parent: OpNode, group_cols: list,
                 agg_col: rel.Column, aggregator: str, eq_flag_op: OpNode, sorted_keys_op: OpNode):

        super(IndexAggregate, self).__init__(out_rel, parent, group_cols, agg_col, aggregator)
        self.eq_flag_op = eq_flag_op
        self.sorted_keys_op = sorted_keys_op

    @classmethod
    def from_aggregate(cls, agg_op: Aggregate, eq_flag_op: OpNode, sorted_keys_op: OpNode):

        obj = cls(agg_op.out_rel, agg_op.parent, agg_op.group_cols,
                  agg_op.agg_col, agg_op.aggregator, eq_flag_op, sorted_keys_op)
        return obj


class Project(UnaryOpNode):

    def __init__(self, out_rel: rel.Relation, parent: OpNode, selected_cols: list):

        super(Project, self).__init__("project", out_rel, parent)
        # Projections can be done by parties locally
        self.is_local = True
        self.selected_cols = selected_cols

    def is_reversible(self):

        # slightly oversimplified but basically if we have
        # re-ordered the input columns without dropping any cols
        # then this is reversible
        return len(self.selected_cols) == len(self.get_in_rel().columns)

    def update_op_specific_cols(self):

        temp_cols = self.get_in_rel().columns
        self.selected_cols = [temp_cols[col.idx] for col in temp_cols]


class Index(UnaryOpNode):
    """Add a column with row indeces to relation"""

    def __init__(self, out_rel: rel.Relation, parent: OpNode):

        super(Index, self).__init__("index", out_rel, parent)
        # Indexing needs parties to communicate size
        self.is_local = False

    def is_reversible(self):

        return True


class Shuffle(UnaryOpNode):
    """Randomly permute rows of relation"""

    def __init__(self, out_rel: rel.Relation, parent: OpNode):

        super(Shuffle, self).__init__("shuffle", out_rel, parent)
        self.is_local = False

    def is_reversible(self):
        # Order is broken, but all the values are still there
        return True


class Multiply(UnaryOpNode):

    def __init__(self, out_rel: rel.Relation, parent: OpNode, target_col: rel.Column, operands: list):

        super(Multiply, self).__init__("multiply", out_rel, parent)
        self.operands = operands
        self.target_col = target_col
        self.is_local = True

    def is_reversible(self):

        # A multiplication is reversible unless one of the operands is 0
        # TODO: is this true?
        return all([op != 0 for op in self.operands])

    def update_op_specific_cols(self):

        temp_cols = self.get_in_rel().columns
        old_operands = copy.copy(self.operands)
        self.operands = [temp_cols[col.idx] if isinstance(
            col, rel.Column) else col for col in old_operands]


class SortBy(UnaryOpNode):

    def __init__(self, out_rel: rel.Relation, parent: OpNode, sort_by_col: rel.Column):

        super(SortBy, self).__init__("sortBy", out_rel, parent)
        self.sort_by_col = sort_by_col

    def update_op_specific_cols(self):

        self.sort_by_col = self.get_in_rel().columns[self.sort_by_col.idx]


class CompNeighs(UnaryOpNode):

    def __init__(self, out_rel: rel.Relation, parent: OpNode, comp_col: rel.Column):

        super(CompNeighs, self).__init__("compNeighs", out_rel, parent)
        self.comp_col = comp_col

    def update_op_specific_cols(self):

        self.comp_col = self.get_in_rel().columns[self.comp_col.idx]


class Distinct(UnaryOpNode):

    def __init__(self, out_rel: rel.Relation, parent: OpNode, selected_cols: list):

        super(Distinct, self).__init__("distinct", out_rel, parent)
        self.selected_cols = selected_cols

    def update_op_specific_cols(self):

        temp_cols = self.get_in_rel().columns
        old_operands = copy.copy(self.operands)
        self.operands = [temp_cols[col.idx] if isinstance(
            col, rel.Column) else col for col in old_operands]


class Divide(UnaryOpNode):

    def __init__(self, out_rel: rel.Relation, parent: OpNode, target_col: rel.Column, operands: list):

        super(Divide, self).__init__("divide", out_rel, parent)
        self.operands = operands
        self.target_col = target_col
        self.is_local = True

    def is_reversible(self):

        return True

    def update_op_specific_cols(self):

        temp_cols = self.get_in_rel().columns
        old_operands = copy.copy(self.operands)
        self.operands = [temp_cols[col.idx] if isinstance(
            col, rel.Column) else col for col in old_operands]


class Filter(UnaryOpNode):

    # TODO: (ben) type annotations on 'operator' and 'expr'
    def __init__(self, out_rel: rel.Relation, parent: OpNode, target_col: rel.Column, operator: str, expr: str):

        super(Filter, self).__init__("filter", out_rel, parent)
        self.operator = operator
        self.filter_expr = expr
        self.target_col = target_col
        self.is_local = True

    def is_reversible(self):

        return False


class Join(BinaryOpNode):

    def __init__(self, out_rel: rel.Relation, left_parent: OpNode,
                 right_parent: OpNode, left_join_cols: list, right_join_cols: list):

        super(Join, self).__init__("join", out_rel, left_parent, right_parent)
        self.left_join_cols = left_join_cols
        self.right_join_cols = right_join_cols

    def update_op_specific_cols(self):

        self.left_join_cols = [self.get_left_in_rel().columns[left_join_col.idx]
                               for left_join_col in copy.copy(self.left_join_cols)]
        self.right_join_cols = [self.get_right_in_rel().columns[right_join_col.idx]
                                for right_join_col in copy.copy(self.right_join_cols)]


class IndexJoin(Join):
    """TODO"""

    def __init__(self, out_rel: rel.Relation, left_parent: OpNode, right_parent: OpNode,
                 left_join_cols: list, right_join_cols: list, index_rel: rel.Relation):

        super(IndexJoin, self).__init__(out_rel, left_parent,
                                        right_parent, left_join_cols, right_join_cols)
        self.name = "indexJoin"
        self.index_rel = index_rel
        # index rel is also a parent
        self.parents.add(index_rel)
        self.is_mpc = True

    @classmethod
    def from_join(cls, join_op: Join, index_rel: rel.Relation):
        obj = cls(join_op.out_rel, join_op.left_parent, join_op.right_parent,
                  join_op.left_join_cols, join_op.right_join_cols, index_rel)
        return obj


class RevealJoin(Join):
    """Join Optimization

    applies when the result of a join
    and one of its inputs is known to the same party P. Instead
    of performing a complete oblivious join, all the rows
    of the other input relation can be revealed to party P,
    provided that their key column a key in P's input.
    """

    # TODO: (ben) recipient == pid (int) ?
    def __init__(self, out_rel: rel.Relation, left_parent: OpNode, right_parent: OpNode,
                 left_join_cols: list, right_join_cols: list, revealed_in_rel: rel.Relation, recepient):

        super(RevealJoin, self).__init__(out_rel, left_parent,
                                         right_parent, left_join_cols, right_join_cols)
        self.name = "revealJoin"
        self.revealed_in_rel = revealed_in_rel
        self.recepient = recepient
        self.is_mpc = True

    @classmethod
    def from_join(cls, join_op: Join, revealed_in_rel: rel.Relation, recepient):
        obj = cls(join_op.out_rel, join_op.left_parent, join_op.right_parent,
                  join_op.left_join_cols, join_op.right_join_cols, revealed_in_rel, recepient)
        return obj

    # TODO: remove?
    def update_op_specific_cols(self):

        self.left_join_cols = [self.get_left_in_rel().columns[c.idx]
                               for c in self.left_join_cols]
        self.right_join_cols = [self.get_right_in_rel().columns[c.idx]
                                for c in self.right_join_cols]


class HybridJoin(Join):
    """Join Optimization

    applies when there exists a singleton collusion set on both
    input key columns, meaning that said party can learn all values
    in both key columns
    """

    # TODO: (ben) trusted_party == pid (int) ?
    def __init__(self, out_rel: rel.Relation, left_parent: OpNode, right_parent: OpNode,
                 left_join_cols: list, right_join_cols: list, trusted_party):

        super(HybridJoin, self).__init__(out_rel, left_parent,
                                         right_parent, left_join_cols, right_join_cols)
        self.name = "hybridJoin"
        self.trusted_party = trusted_party
        self.is_mpc = True

    @classmethod
    def from_join(cls, join_op: Join, trusted_party):
        obj = cls(join_op.out_rel, join_op.left_parent, join_op.right_parent,
                  join_op.left_join_cols, join_op.right_join_cols, trusted_party)
        obj.children = join_op.children
        for child in obj.children:
            child.replace_parent(join_op, obj)
        return obj


class Dag:

    def __init__(self, roots: set):

        self.roots = roots

    # TODO: (ben) type of visitor?
    def _dfs_visit(self, node: OpNode, visitor, visited: set):

        visitor(node)
        visited.add(node)
        for child in node.children:
            if child not in visited:
                self._dfs_visit(child, visitor, visited)

    def dfs_visit(self, visitor):

        visited = set()

        for root in self.roots:
            self._dfs_visit(root, visitor, visited)

        return visited

    def dfs_print(self):

        self.dfs_visit(print)

    def get_all_nodes(self):

        return self.dfs_visit(lambda node: node)

    # Note: not optimized at all but we're dealing with very small
    # graphs so performance shouldn't be a problem
    # Side-effects on all inputs other than node
    def _top_sort_visit(self, node: OpNode, marked: set, temp_marked: set,
                        unmarked, ordered: list, deterministic: bool = True):

        if node in temp_marked:
            raise Exception("Not a Dag!")

        if node not in marked:
            if node in unmarked:
                unmarked.remove(node)
            temp_marked.add(node)

            children = node.children
            if deterministic:
                children = sorted(list(children), key=lambda x: x.out_rel.name)
            for other_node in children:
                self._top_sort_visit(
                    other_node, marked, temp_marked, unmarked, ordered)

            marked.add(node)
            if deterministic:
                unmarked.append(node)
            else:
                unmarked.add(node)
            temp_marked.remove(node)
            ordered.insert(0, node)

    # TODO: the deterministic flag is a hack, come up with something more elegant
    def top_sort(self, deterministic: bool = True):

        unmarked = self.get_all_nodes()

        if deterministic:
            unmarked = sorted(list(unmarked), key=lambda x: x.out_rel.name)
        marked = set()
        temp_marked = set()
        ordered = []

        while unmarked:

            node = unmarked.pop()
            self._top_sort_visit(node, marked, temp_marked, unmarked, ordered)

        return ordered


class OpDag(Dag):

    def __init__(self, roots: set):

        super(OpDag, self).__init__(roots)

    def __str__(self):

        order = self.top_sort()
        return ",\n".join(str(node) for node in order)


def remove_between(parent: OpNode, child: OpNode, other: OpNode):

    assert len(other.children) < 2
    assert len(other.parents) < 2
    # only dealing with unary nodes for now
    assert isinstance(other, UnaryOpNode)

    if child:
        child.replace_parent(other, parent)
        child.update_op_specific_cols()
        parent.replace_child(other, child)
    else:
        parent.children.remove(other)

    other.make_orphan()
    other.children = set()


def insert_between_children(parent: OpNode, other: OpNode):

    assert not other.children
    assert not other.parents
    # only dealing with unary nodes for now
    assert isinstance(other, UnaryOpNode)

    other.parent = parent
    other.parents.add(parent)

    children = copy.copy(parent.children)
    for child in children:
        child.replace_parent(parent, other)
        if child in parent.children:
            parent.children.remove(child)
        child.update_op_specific_cols()
        other.children.add(child)

    parent.children.add(other)


def insert_between(parent: OpNode, child: OpNode, other: OpNode):

    # called with grandParent, topNode, toInsert
    assert(not other.children)
    assert(not other.parents)
    # only dealing with unary nodes for now
    assert(isinstance(other, UnaryOpNode))

    # Insert other below parent
    other.parents.add(parent)
    other.parent = parent
    parent.children.add(other)
    other.update_op_specific_cols()

    # Remove child from parent
    if child:
        child.replace_parent(parent, other)
        if child in parent.children:
            parent.children.remove(child)
        child.update_op_specific_cols()
        other.children.add(child)
