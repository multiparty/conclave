from conclave.dag import *


class CodeGen:
    """ Base class for code generation. """

    def __init__(self, config, dag):
        """ Initialize CodeGen with DAG and config. """

        self.config = config
        self.dag = dag

    def generate(self, job_name: str, output_directory: str):
        """ Generate code for DAG passed and write to file. """

        job, code = self._generate(job_name, output_directory)
        # store the code in type-specific files
        self._write_code(code, job_name)
        # return job object
        return job

    def _generate(self, job_name: [str, None], output_directory: [str, None]):
        """ Generate code for DAG passed"""

        op_code = ""

        # topological traversal
        nodes = self.dag.top_sort()

        # TODO: handle subclassing more gracefully
        # for each op
        for node in nodes:
            if node.skip:
                print("Skipping inactive node", node)
                continue
            if isinstance(node, HybridAggregate):
                op_code += self._generate_hybrid_aggregate(node)
            elif isinstance(node, LeakyIndexAggregate):
                op_code += self._generate_leaky_hybrid_aggregate(node)
            elif isinstance(node, IndexAggregate):
                op_code += self._generate_index_aggregate(node)
            elif isinstance(node, Aggregate):
                op_code += self._generate_aggregate(node)
            elif isinstance(node, Concat):
                op_code += self._generate_concat(node)
            elif isinstance(node, Create):
                op_code += self._generate_create(node)
            elif isinstance(node, Close):
                op_code += self._generate_close(node)
            elif isinstance(node, JoinFlags):
                op_code += self._generate_join_flags(node)
            elif isinstance(node, FlagJoin):
                op_code += self._generate_flag_join(node)
            elif isinstance(node, IndexJoin):
                op_code += self._generate_index_join(node)
            elif isinstance(node, PublicJoin):
                op_code += self._generate_public_join(node)
            elif isinstance(node, HybridJoin):
                op_code += self._generate_hybrid_join(node)
            elif isinstance(node, Join):
                op_code += self._generate_join(node)
            elif isinstance(node, Open):
                op_code += self._generate_open(node)
            elif isinstance(node, Filter):
                op_code += self._generate_filter(node)
            elif isinstance(node, Project):
                op_code += self._generate_project(node)
            elif isinstance(node, Persist):
                op_code += self._generate_persist(node)
            elif isinstance(node, Multiply):
                op_code += self._generate_multiply(node)
            elif isinstance(node, Divide):
                op_code += self._generate_divide(node)
            elif isinstance(node, Index):
                op_code += self._generate_index(node)
            elif isinstance(node, Shuffle):
                op_code += self._generate_shuffle(node)
            elif isinstance(node, Distinct):
                op_code += self._generate_distinct(node)
            elif isinstance(node, DistinctCount):
                op_code += self._generate_distinct_count(node)
            elif isinstance(node, SortBy):
                op_code += self._generate_sort_by(node)
            elif isinstance(node, CompNeighs):
                op_code += self._generate_comp_neighs(node)
            elif isinstance(node, PubJoin):
                op_code += self._generate_pub_join(node)
            elif isinstance(node, ConcatCols):
                op_code += self._generate_concat_cols(node)
            elif isinstance(node, FilterBy):
                op_code += self._generate_filter_by(node)
            elif isinstance(node, Union):
                op_code += self._generate_union(node)
            elif isinstance(node, PubIntersect):
                op_code += self._generate_pub_intersect(node)
            elif isinstance(node, IndexesToFlags):
                op_code += self._generate_indexes_to_flags(node)
            elif isinstance(node, NumRows):
                op_code += self._generate_num_rows(node)
            elif isinstance(node, Blackbox):
                op_code += self._generate_blackbox(node)
            else:
                print("encountered unknown operator type", repr(node))

        # expand top-level job template and return code
        return self._generate_job(job_name, self.config.code_path, op_code)

    def _write_code(self, code, job_name):
        """ Overridden in subclasses. """
        pass
