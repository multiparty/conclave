"""
Data structure for representing a workflow directed acyclic graph (DAG).
"""
import copy

from conclave import rel


class Node:
    """
    Graph node data structure.
    """

    def __init__(self, name: str):
        """ Initalize graph node object. """
        self.name = name
        self.children = set()
        self.parents = set()

    def debug_str(self):
        """ Return extended string representation for debugging. """
        children_str = str([n.name for n in self.children])
        parent_str = str([n.name for n in self.parents])
        return self.name + " children: " + children_str + " parents: " + parent_str

    def is_leaf(self):
        """ Return whether node is a leaf. """
        return len(self.children) == 0

    def is_root(self):
        """ Return whether node is a root. """
        return len(self.parents) == 0

    def __str__(self):
        """ Return string representation of node. """
        return self.name


class OpNode(Node):
    """
    Base class for nodes that store relational operations
    """

    def __init__(self, name: str, out_rel: rel.Relation):
        """ Initialize OpNode object. """
        super(OpNode, self).__init__(name)
        self.out_rel = out_rel
        # By default we assume that the operator requires data
        # to cross party boundaries. Override this for operators
        # where this is not the case
        self.is_local = False
        self.is_mpc = False
        self.skip = False

    def is_boundary(self):
        """ Returns whether this node is at an MPC boundary. """
        # TODO: could this be (self.is_upper_boundary() or
        # self.is_lower_boundary())?
        return self.is_upper_boundary()

    def is_upper_boundary(self):
        """ Returns whether this node is MPC and is at the start of an MPC job. """
        return self.is_mpc and not any([par.is_mpc and not isinstance(par, Close) for par in self.parents])

    def is_lower_boundary(self):
        """ Returns whether this node is MPC and is at the end of an MPC job. """
        return self.is_mpc and not any([child.is_mpc and not isinstance(child, Open) for child in self.children])

    def is_reversible(self):
        """
        Reversible in the sense that, given the output of the operation, we reconstruct it's inputs. An
        example of this property could be Multiplication, where if you have the output and knowledge
        that the second column was multiplied by 3, you could reconstruct the original column. An example
        of a non-reversible operation is Aggregation, where you cannot infer the original data given only
        the output, the aggregator, and the columns that were grouped over. At present, we consider whether
        an entire relation is reversible as opposed to column-level reversibility. OpNodes are not reversible
        by default.
        """
        return False

    def requires_mpc(self):
        """ All operations require MPC by default. """
        return True

    def update_op_specific_cols(self):
        """ Overridden in subclasses. """
        return

    def update_out_rel_cols(self):
        """ Overridden in subclasses. """
        return

    def update_stored_with(self):
        """ Overridden in subclasses. """
        return

    def make_orphan(self):
        """ Remove link between this node and it's parent nodes. """
        self.parents = set()

    def remove_parent(self, parent: Node):
        """ Remove link between this node and a specific parent node. """
        self.parents.remove(parent)

    def replace_parent(self, old_parent: Node, new_parent: Node):
        """ Replace a specific parent of this node with another parent node. """
        self.parents.remove(old_parent)
        self.parents.add(new_parent)

    def replace_child(self, old_child: Node, new_child: Node):
        """ Replace a specific child of this node with another child node. """
        self.children.remove(old_child)
        self.children.add(new_child)

    def get_sorted_children(self):
        """ Return a list of this node's child nodes in alphabetical order. """
        return sorted(list(self.children), key=lambda x: x.out_rel.name)

    def get_sorted_parents(self):
        """ Return a list of this node's parent nodes in alphabetical order. """
        return sorted(list(self.parents), key=lambda x: x.out_rel.name)

    def __str__(self):
        """ Return a string representation of this node and whether it requires MPC. """
        return "{}{}->{}".format(
            super(OpNode, self).__str__(),
            "mpc" if self.is_mpc else "",
            self.out_rel.name
        )


class UnaryOpNode(OpNode):
    """ An OpNode with exactly one parent (e.g. - Multiply, Project, etc.). """

    def __init__(self, name: str, out_rel: rel.Relation, parent: OpNode):
        """ Initialize UnaryOpNode object. """
        super(UnaryOpNode, self).__init__(name, out_rel)
        self.parent = parent
        if self.parent:
            self.parents.add(parent)

    def get_in_rel(self):
        """ Returns out_rel of parent node. """
        return self.parent.out_rel

    def requires_mpc(self):
        """ Returns whether the in_rel of this node requires MPC. """
        return self.get_in_rel().is_shared() and not self.is_local

    def update_stored_with(self):
        """ Returns the set of parties who store the data in this operation locally. """
        self.out_rel.stored_with = copy.copy(self.get_in_rel().stored_with)

    def make_orphan(self):
        """ Remove the link between the node and it's parent node. """
        super(UnaryOpNode, self).make_orphan()
        self.parent = None

    def replace_parent(self, old_parent: OpNode, new_parent: OpNode):
        """ Replace this node's parent with another node. """
        super(UnaryOpNode, self).replace_parent(old_parent, new_parent)
        self.parent = new_parent

    def remove_parent(self, parent):
        """ Remove this node's parent. """
        super(UnaryOpNode, self).remove_parent(parent)
        self.parent = None


class BinaryOpNode(OpNode):
    """ An OpNode with exactly two parents (e.g. - Join). """

    def __init__(self, name: str, out_rel: rel.Relation, left_parent: OpNode, right_parent: OpNode):

        super(BinaryOpNode, self).__init__(name, out_rel)
        self.left_parent = left_parent
        self.right_parent = right_parent
        if self.left_parent:
            self.parents.add(left_parent)
        if self.right_parent:
            self.parents.add(right_parent)

    def get_left_in_rel(self):
        """ Returns left input relation to this node. """
        return self.left_parent.out_rel

    def get_right_in_rel(self):
        """ Returns right input relation to this ndoe. """
        return self.right_parent.out_rel

    def requires_mpc(self):
        """
        Returns whether the union of the input relations to this node are owned by the
        same party. If more than one party owns this union, this method returns True.
        """
        left_stored_with = self.get_left_in_rel().stored_with
        right_stored_with = self.get_right_in_rel().stored_with
        combined = left_stored_with.union(right_stored_with)
        return (len(combined) > 1) and not self.is_local

    def make_orphan(self):
        """ Removes link between this node and both of it's parents. """
        super(BinaryOpNode, self).make_orphan()
        self.left_parent = None
        self.right_parent = None

    def replace_parent(self, old_parent: Node, new_parent: OpNode):
        """ Replace either the left or the right parent of this node. """
        super(BinaryOpNode, self).replace_parent(old_parent, new_parent)
        if self.left_parent == old_parent:
            self.left_parent = new_parent
        elif self.right_parent == old_parent:
            self.right_parent = new_parent

    def remove_parent(self, parent: OpNode):
        """ Remove the link between this node and one of it's parents. """
        super(BinaryOpNode, self).remove_parent(parent)
        if self.left_parent == parent:
            self.left_parent = None
        elif self.right_parent == parent:
            self.right_parent = None


class NaryOpNode(OpNode):
    """ An OpNode with arbitrarily many parents (e.g. - Concat)."""

    def __init__(self, name: str, out_rel: rel.Relation, parents: set):
        """ Initialize NaryOpNode object. """
        super(NaryOpNode, self).__init__(name, out_rel)
        self.parents = parents

    def get_in_rels(self):
        """
        Returns input relations to this node. A set is returned to emphasize that the order
        of the returned relations is meaningless, since the parent-set that the relations come
        from isn't ordered. If an operator where ordering on input relations matters is needed,
        it will be implemented as a separate class.
        """
        return set([parent.out_rel for parent in self.parents])

    def requires_mpc(self):
        """
        Returns whether the union of the input relations to this node are owned by the
        same party. If more than one party owns this union, this method returns True.
        """
        in_coll_sets = [in_rel.stored_with for in_rel in self.get_in_rels()]
        in_rels_shared = len(set().union(*in_coll_sets)) > 1
        return in_rels_shared and not self.is_local


class Create(UnaryOpNode):
    """ Object for creating datasets in a DAG. """

    def __init__(self, out_rel: rel.Relation):
        """ Initialize Create object. """
        super(Create, self).__init__("create", out_rel, None)
        # Input can be done by parties locally
        self.is_local = True

    def requires_mpc(self):
        """ Create operations are always done locally and will never require MPC. """
        return False


class Store(UnaryOpNode):
    """ Object for storing data returned by a workflow. """

    def __init__(self, out_rel: rel.Relation, parent: OpNode):
        """ Initialize Store object. """
        super(Store, self).__init__("store", out_rel, parent)

    def is_reversible(self):
        """
        Store ops are reversible by definition, since the output of a Store
        operation is identical to it's input.
        """
        return True


class Persist(UnaryOpNode):
    """ ??? """

    def __init__(self, out_rel: rel.Relation, parent: OpNode):
        super(Persist, self).__init__("persist", out_rel, parent)

    def is_reversible(self):
        return True


class Open(UnaryOpNode):
    """ Object for opening results of a computation to participating parties. """

    def __init__(self, out_rel: rel.Relation, parent: [OpNode, None]):
        """ Initialize Open object. """
        super(Open, self).__init__("open", out_rel, parent)
        self.is_mpc = True

    def is_reversible(self):
        """ Open is always reversible for the same reason that Store is reversible. """
        return True


class Close(UnaryOpNode):
    """ Object for marking the boundary between local and MPC operations. """

    def __init__(self, out_rel: rel.Relation, parent: [OpNode, None]):
        """ Initialize Close object. """
        super(Close, self).__init__("close", out_rel, parent)
        self.is_mpc = True

    def is_reversible(self):
        """ Inputs to a Close operation can be reconstructed via combining the output shares. """
        return True


class Send(UnaryOpNode):
    """ Object for Sending secret shares to another party. """

    def __init__(self, out_rel: rel.Relation, parent: OpNode):
        """ Initialize Send object."""
        super(Send, self).__init__("send", out_rel, parent)

    def is_reversible(self):
        """ The outputs of a Send operation are identical to it's inputs. """
        return True


class ConcatCols(NaryOpNode):
    """ Object to store the concatenation of several relations' columns. """

    def __init__(self, out_rel: rel.Relation, parents: list, use_mult: bool):
        """ Initialize a ConcatCols object. """
        parent_set = set(parents)
        # sanity check for now
        assert (len(parents) == len(parent_set))
        super(ConcatCols, self).__init__("concat_cols", out_rel, parent_set)
        self.ordered = parents
        self.use_mult = use_mult
        self.is_local = False

    def is_reversible(self):
        """ TODO pre-deadline hack."""
        return False

    def get_in_rels(self):
        """ Returns the list of input relations to this node. """
        return [parent.out_rel for parent in self.ordered]

    def replace_parent(self, old_parent: OpNode, new_parent: OpNode):
        """ Replace a particular parent node with another. """
        super(ConcatCols, self).replace_parent(old_parent, new_parent)
        # this will throw if old_parent not in list
        idx = self.ordered.index(old_parent)
        self.ordered[idx] = new_parent

    def remove_parent(self, parent: OpNode):
        # TODO
        raise NotImplementedError()


class Concat(NaryOpNode):
    """ Object to store the concatenation of several relations' datasets. """

    def __init__(self, out_rel: rel.Relation, parents: list):
        """ Initialize a Concat object. """
        parent_set = set(parents)
        # sanity check for now
        assert (len(parents) == len(parent_set))
        # only handle same class input operators (otherwise managing columns during rewrites becomes annoying)
        assert (len(set([type(x).__name__ for x in parents])) == 1)
        super(Concat, self).__init__("concat", out_rel, parent_set)
        self.ordered = parents

    def is_reversible(self):
        """ No data is changed during a Concat operation."""
        return True

    def get_in_rels(self):
        """ Returns the list of input relations to this node. """
        return [parent.out_rel for parent in self.ordered]

    def replace_parent(self, old_parent: OpNode, new_parent: OpNode):
        """ Replace a particular parent node with another. """
        # TODO figure out how to enforce that all parents remain same class
        super(Concat, self).replace_parent(old_parent, new_parent)
        # this will throw if old_parent not in list
        idx = self.ordered.index(old_parent)
        self.ordered[idx] = new_parent

    def remove_parent(self, parent: OpNode):
        # TODO
        raise NotImplementedError()

    def update_out_rel_cols(self):
        in_rel_cols = copy.deepcopy(self.get_in_rels()[0].columns)
        self.out_rel.columns = in_rel_cols
        self.out_rel.update_columns()


class Blackbox(NaryOpNode):
    """ Blackbox operator for backend-specific functionality """

    def __init__(self, out_rel: rel.Relation, parents: list, backend: str, code: str):
        """ Initialize a Blackbox object. """
        parent_set = set(parents)
        super(Blackbox, self).__init__("blackbox", out_rel, parent_set)
        self.ordered = parents
        self.backend = backend
        self.code = code


class Aggregate(UnaryOpNode):
    """ Object to store an aggregation over data. """

    def __init__(self, out_rel: rel.Relation, parent: OpNode,
                 group_cols: list, agg_col: [rel.Column, None], aggregator: str):
        """ Initialize Aggregate object. """
        if aggregator not in {"sum", "count", "mean", "std_dev"}:
            raise Exception("Unsupported aggregator {}".format(aggregator))
        super(Aggregate, self).__init__("aggregation", out_rel, parent)
        self.group_cols = group_cols
        if aggregator == "count" and agg_col:
            raise Exception("Don't supply agg_col for count")
        self.agg_col = agg_col
        self.aggregator = aggregator

    def update_op_specific_cols(self):
        """ Update this node's group_cols and agg_col based on the columns of its input relation. """
        self.group_cols = [self.get_in_rel().columns[group_col.idx]
                           for group_col in self.group_cols]
        if self.aggregator != "count":
            self.agg_col = self.get_in_rel().columns[self.agg_col.idx]


class IndexAggregate(Aggregate):
    """ Object to store an indexed aggregation operation. """

    def __init__(self, out_rel: rel.Relation, parent: OpNode, group_cols: list,
                 agg_col: rel.Column, aggregator: str, eq_flag_op: OpNode, sorted_keys_op: OpNode):
        """ Initialize IndexAggregate object. """
        super(IndexAggregate, self).__init__(out_rel, parent, group_cols, agg_col, aggregator)
        self.eq_flag_op = eq_flag_op
        self.sorted_keys_op = sorted_keys_op

    @classmethod
    def from_aggregate(cls, agg_op: Aggregate, eq_flag_op: OpNode, sorted_keys_op: OpNode):
        """ Generate an IndexAggregate object from an Aggregate object. """
        obj = cls(agg_op.out_rel, agg_op.parent, agg_op.group_cols,
                  agg_op.agg_col, agg_op.aggregator, eq_flag_op, sorted_keys_op)
        return obj


class LeakyIndexAggregate(Aggregate):
    """ Object to store an leaky indexed aggregation operation. """

    def __init__(self, out_rel: rel.Relation, parent: OpNode, group_cols: list,
                 agg_col: rel.Column, aggregator: str, dist_keys: OpNode, keys_to_idx_map: OpNode):
        """ Initialize LeakyIndexAggregate object. """
        super(LeakyIndexAggregate, self).__init__(out_rel, parent, group_cols, agg_col, aggregator)
        self.dist_keys = dist_keys
        self.keys_to_idx_map = keys_to_idx_map

    @classmethod
    def from_aggregate(cls, agg_op: Aggregate, eq_flag_op: OpNode, sorted_keys_op: OpNode):
        """ Generate an LeakyIndexAggregate object from an Aggregate object. """
        obj = cls(agg_op.out_rel, agg_op.parent, agg_op.group_cols,
                  agg_op.agg_col, agg_op.aggregator, eq_flag_op, sorted_keys_op)
        return obj


class Project(UnaryOpNode):
    """ Object to store a project operation. """

    def __init__(self, out_rel: rel.Relation, parent: OpNode, selected_cols: list):
        """ Initialize project object. """
        super(Project, self).__init__("project", out_rel, parent)
        # Projections can be done by parties locally
        self.is_local = True
        self.selected_cols = selected_cols

    def is_reversible(self):
        """
        Is reversible if no columns have been dropped from the input relation.
        """
        return len(self.selected_cols) == len(self.get_in_rel().columns)

    def update_op_specific_cols(self):
        """
        Update this node's selected_cols with the columns
        from it's input relation whose idx's match.
        """
        temp_cols = self.get_in_rel().columns
        self.selected_cols = [temp_cols[col.idx] for col in self.selected_cols]


class Index(UnaryOpNode):
    """ Add a column with row indeces to relation. """

    def __init__(self, out_rel: rel.Relation, parent: OpNode, idx_col_name: str):
        """ Initialize Index object"""
        super(Index, self).__init__("index", out_rel, parent)
        # Indexing needs parties to communicate size
        self.is_local = False
        self.idx_col_name = idx_col_name

    def is_reversible(self):
        """ Output is just the input with an index column appended to it. """
        return True


class NumRows(UnaryOpNode):
    """ Output num rows of input to a relation. """

    def __init__(self, out_rel: rel.Relation, parent: OpNode, col_name: str):
        """ Initialize NumRows object"""
        super(NumRows, self).__init__("num_rows", out_rel, parent)
        # Need to communicate size so can't be local
        self.is_local = False
        self.col_name = col_name

    def is_reversible(self):
        return True


class Shuffle(UnaryOpNode):
    """ Randomly permute rows of relation. """

    def __init__(self, out_rel: rel.Relation, parent: OpNode):
        """ Initialize Shuffle object. """
        super(Shuffle, self).__init__("shuffle", out_rel, parent)
        self.is_local = False

    def is_reversible(self):
        """ Order is broken, but values are the same. """
        return True


class Multiply(UnaryOpNode):
    """ Object to store multiplication between columns. """

    def __init__(self, out_rel: rel.Relation, parent: OpNode, target_col: rel.Column, operands: list):
        """ Initialize Multiply object. """
        super(Multiply, self).__init__("multiply", out_rel, parent)
        self.operands = operands
        self.target_col = target_col
        self.is_local = True

    def is_reversible(self):
        """ Reversible if none of the operands equal 0. """
        return all([op != 0 for op in self.operands])

    def update_op_specific_cols(self):
        """
        Updates this node's operand columns with the columns
        from it's input relation.
        """
        temp_cols = self.get_in_rel().columns
        old_operands = copy.copy(self.operands)
        self.operands = [temp_cols[col.idx] if isinstance(
            col, rel.Column) else col for col in old_operands]


class Limit(UnaryOpNode):
    """ Object to store the limiting of resulting rows from a relation. """

    def __init__(self, out_rel: rel.Relation, parent: OpNode, num: int):
        super(Limit, self).__init__("limit", out_rel, parent)
        self.num = num

    def is_reversible(self):
        return False


class SortBy(UnaryOpNode):
    """ Object to store the sorting of a relation over a particular column. """

    def __init__(self, out_rel: rel.Relation, parent: OpNode, sort_by_col: rel.Column):
        """ Initialize SortBy object. """
        super(SortBy, self).__init__("sortBy", out_rel, parent)
        self.sort_by_col = sort_by_col

    def update_op_specific_cols(self):
        """
        Updates this node's sort_by_col with the column of it's input relation with matching idx.
        """
        self.sort_by_col = self.get_in_rel().columns[self.sort_by_col.idx]


class CompNeighs(UnaryOpNode):
    """
    Object that stores equality comparison between neighboring row values in a relation.
    Used in Hybrid Aggregation to allow parties to obliviously aggregate during MPC.
    """

    def __init__(self, out_rel: rel.Relation, parent: OpNode, comp_col: rel.Column):
        """ Initialize CompNeighs object. """
        super(CompNeighs, self).__init__("compNeighs", out_rel, parent)
        self.comp_col = comp_col

    def update_op_specific_cols(self):
        """
        Updates this node's comp_col with the column of it's input relation with matching idx.
        """
        self.comp_col = self.get_in_rel().columns[self.comp_col.idx]


class Distinct(UnaryOpNode):
    """
    Object that stores removing duplicate rows from a relation,
    with respect to a set of selected columns.
    """

    def __init__(self, out_rel: rel.Relation, parent: OpNode, selected_cols: list):
        """ Initialize Distinct object. """
        super(Distinct, self).__init__("distinct", out_rel, parent)
        self.selected_cols = selected_cols

    def update_op_specific_cols(self):
        temp_cols = self.get_in_rel().columns
        old_cols = copy.copy(self.selected_cols)
        self.selected_cols = [temp_cols[col.idx] if isinstance(
            col, rel.Column) else col for col in old_cols]


class DistinctCount(UnaryOpNode):
    """
    Distinct count operator.
    """

    def __init__(self, out_rel: rel.Relation, parent: OpNode, selected_col: str):
        super(DistinctCount, self).__init__("distinct_count", out_rel, parent)
        self.selected_col = selected_col
        self.is_reversible = False
        self.use_sort = True

    def update_op_specific_cols(self):
        temp_cols = self.get_in_rel().columns
        # TODO shouldn't be a copy
        old_col = copy.copy(self.selected_col)
        self.selected_col = temp_cols[old_col.idx] if isinstance(old_col, rel.Column) else old_col


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
        # TODO shouldn't be a copy
        old_operands = copy.copy(self.operands)
        self.operands = [temp_cols[col.idx] if isinstance(col, rel.Column) else col for col in old_operands]


class Filter(UnaryOpNode):
    """
    Operator for filtering relations for rows with specified attribute values.
    """

    def __init__(self, out_rel: rel.Relation, parent: OpNode, filter_col: rel.Column, operator: str,
                 other_col: rel.Column, scalar: int):
        super(Filter, self).__init__("filter", out_rel, parent)
        self.is_scalar = other_col is None
        if self.is_scalar:
            assert scalar is not None
        else:
            assert scalar is None
        self.other_col = other_col
        self.scalar = scalar
        self.operator = operator
        self.filter_col = filter_col
        self.is_local = False

    def is_reversible(self):
        return False

    def update_op_specific_cols(self):
        temp_cols = self.get_in_rel().columns
        self.filter_col = temp_cols[self.filter_col.idx]
        if not self.is_scalar:
            self.other_col = temp_cols[self.other_col.idx]


# TODO rename
class PubJoin(BinaryOpNode):

    def __init__(self, out_rel: rel.Relation, parent: OpNode, key_col: rel.Column, host: str, port: int,
                 is_server: bool, other_op_node: OpNode):
        super(PubJoin, self).__init__("pub_join", out_rel, parent, other_op_node)
        self.key_col = key_col
        self.host = host
        self.port = port
        self.is_server = is_server
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
                               for left_join_col in self.left_join_cols]
        self.right_join_cols = [self.get_right_in_rel().columns[right_join_col.idx]
                                for right_join_col in self.right_join_cols]


class FilterBy(BinaryOpNode):
    """
    Operator for filtering relations for rows which are in a set of values (specified in another relation).
    """

    def __init__(self, out_rel: rel.Relation, input_op_node: OpNode,
                 by_op: OpNode, filter_col: rel.Column, use_not_in: bool):
        if len(by_op.out_rel.columns) != 1:
            raise Exception("ByOp must have single column output relation")
        super(FilterBy, self).__init__("filter_by", out_rel, input_op_node, by_op)
        self.filter_col = filter_col
        self.use_not_in = use_not_in

    def update_op_specific_cols(self):
        temp_cols = self.get_left_in_rel().columns
        self.filter_col = temp_cols[self.filter_col.idx]


class IndexesToFlags(BinaryOpNode):
    """
    TODO
    """

    def __init__(self, out_rel: rel.Relation, input_op_node: OpNode,
                 lookup_op_node: OpNode, stage=0):
        # if len(lookup_op_node.out_rel.columns) != 1:
        #     raise Exception("lookup_op_node must have single column output relation")
        super(IndexesToFlags, self).__init__("indexes_to_flags", out_rel, input_op_node, lookup_op_node)
        self.stage = stage


class Union(BinaryOpNode):
    """
    Operator for union of given columns.
    """

    def __init__(self, out_rel: rel.Relation, left_parent: OpNode,
                 right_parent: OpNode, left_col: rel.Column, right_col: rel.Column):
        super(Union, self).__init__("union", out_rel, left_parent, right_parent)
        self.left_col = left_col
        self.right_col = right_col

    def update_op_specific_cols(self):
        temp_cols = self.get_left_in_rel().columns
        self.left_col = temp_cols[self.left_col.idx]
        self.right_col = temp_cols[self.right_col.idx]


class PubIntersect(UnaryOpNode):
    """
    Operator for intersection of given (public) columns.
    """

    def __init__(self, out_rel: rel.Relation,
                 parent: OpNode,
                 col: rel.Column,
                 host: str,
                 port: int,
                 is_server: bool):
        super(PubIntersect, self).__init__("intersect_pub", out_rel, parent)
        self.col = col
        self.host = host
        self.port = port
        self.is_server = is_server

    def update_op_specific_cols(self):
        temp_cols = self.get_in_rel().columns
        self.col = temp_cols[self.col.idx]


class JoinFlags(Join):
    """
    Operator node which computes a join and expresses the result as a list of binary flags. For each row in the
    cartesian product of the two input relations the result will hold a 1 flag if the row belongs in the result and 0
    otherwise.
    """

    def __init__(self, out_rel: rel.Relation, left_parent: OpNode,
                 right_parent: OpNode, left_join_cols: list, right_join_cols: list):
        super(JoinFlags, self).__init__(out_rel, left_parent,
                                        right_parent, left_join_cols, right_join_cols)
        self.name = "join_flags"
        self.is_mpc = True

    def update_op_specific_cols(self):
        self.left_join_cols = [self.get_left_in_rel().columns[left_join_col.idx]
                               for left_join_col in copy.copy(self.left_join_cols)]
        self.right_join_cols = [self.get_right_in_rel().columns[right_join_col.idx]
                                for right_join_col in copy.copy(self.right_join_cols)]

    @classmethod
    def from_join(cls, join_op: Join):
        obj = cls(join_op.out_rel, join_op.left_parent, join_op.right_parent,
                  join_op.left_join_cols, join_op.right_join_cols)
        return obj


class IndexJoin(Join):
    """TODO"""

    def __init__(self, out_rel: rel.Relation, left_parent: OpNode, right_parent: OpNode,
                 left_join_cols: list, right_join_cols: list, index_op: OpNode):
        super(IndexJoin, self).__init__(out_rel, left_parent,
                                        right_parent, left_join_cols, right_join_cols)
        self.name = "indexJoin"
        self.index_rel = index_op
        # index rel is also a parent
        self.parents.add(index_op)
        self.is_mpc = True

    @classmethod
    def from_join(cls, join_op: Join, index_op: OpNode):
        obj = cls(join_op.out_rel, join_op.left_parent, join_op.right_parent,
                  join_op.left_join_cols, join_op.right_join_cols, index_op)
        return obj


class FlagJoin(Join):
    """
    Operator node which computes the result of a join given two input relations
    and equality flags computed via a JoinFlags operation.
    """

    def __init__(self, out_rel: rel.Relation, left_parent: OpNode, right_parent: OpNode,
                 left_join_cols: list, right_join_cols: list, join_flags_op: OpNode):
        super(FlagJoin, self).__init__(out_rel, left_parent, right_parent, left_join_cols, right_join_cols)
        self.name = "flag_join"
        self.join_flag_op = join_flags_op
        self.parents.add(join_flags_op)
        self.is_mpc = True

    @classmethod
    def from_join(cls, join_op: Join, join_flags_op: OpNode):
        obj = cls(join_op.out_rel, join_op.left_parent, join_op.right_parent,
                  join_op.left_join_cols, join_op.right_join_cols, join_flags_op)
        return obj

    def update_op_specific_cols(self):
        pass


class PublicJoin(Join):

    def __init__(self, out_rel: rel.Relation, left_parent: OpNode, right_parent: OpNode,
                 left_join_cols: list, right_join_cols: list):
        super(PublicJoin, self).__init__(out_rel, left_parent,
                                         right_parent, left_join_cols, right_join_cols)
        self.name = "public_join"

    @classmethod
    def from_join(cls, join_op: Join):
        obj = cls(join_op.out_rel, join_op.left_parent, join_op.right_parent,
                  join_op.left_join_cols, join_op.right_join_cols)
        obj.children = join_op.children
        for child in obj.children:
            child.replace_parent(join_op, obj)
        return obj

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
    def from_join(cls, join_op: Join, trusted_party: int):
        obj = cls(join_op.out_rel, join_op.left_parent, join_op.right_parent,
                  join_op.left_join_cols, join_op.right_join_cols, trusted_party)
        obj.children = join_op.children
        for child in obj.children:
            child.replace_parent(join_op, obj)
        return obj


class HybridAggregate(Aggregate):
    """ Object to store an aggregation assisted by a semi-trusted party over data. """

    def __init__(self, out_rel: rel.Relation, parent: OpNode, group_cols: list, agg_col: rel.Column, aggregator: str,
                 trusted_party: int):
        """ Initialize HybridAggregate object. """
        super(HybridAggregate, self).__init__(out_rel, parent, group_cols, agg_col, aggregator)
        self.name = "hybrid_aggregation"
        self.trusted_party = trusted_party
        self.is_mpc = True

    @classmethod
    def from_aggregate(cls, agg_op: Aggregate, trusted_party: int):
        obj = cls(agg_op.out_rel, agg_op.parent, agg_op.group_cols, agg_op.agg_col, agg_op.aggregator, trusted_party)
        obj.children = agg_op.children
        for child in obj.children:
            child.replace_parent(agg_op, obj)
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
            raise Exception("Not a Dag! Node ", node, " was in ", temp_marked)

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
    """
    Removes other between parent and child.
    """
    assert len(other.children) < 2
    assert len(other.parents) < 2
    # only dealing with unary nodes for now
    assert isinstance(other, UnaryOpNode)

    if child:
        child.replace_parent(other, parent)
        # child.update_op_specific_cols()
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
    """
    Inserts other between parent and child.
    """

    # called with grandParent, topNode, toInsert
    assert (not other.children)
    assert (not other.parents)
    # only dealing with unary nodes for now
    assert (isinstance(other, UnaryOpNode))

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
