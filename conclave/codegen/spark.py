import os
import pystache

import conclave.dag as saldag
from conclave.codegen import CodeGen
from conclave.job import SparkJob


def cache_var(op_node: saldag.OpNode):
    """ Determines whether a Spark DF must be cached. """
    if len(op_node.children) > 1:
        return ".cache()"
    else:
        return ''


def convert_type(type_str: str):
    """ Convert type strings from column definitions to Spark type definitions. """

    if type_str == "INTEGER":
        return "IntegerType()"
    elif type_str == "STRING":
        return "StringType()"
    else:
        raise Exception("Unsupported data type")


class SparkCodeGen(CodeGen):
    """ Codegen subclass for generating Spark code. """

    def __init__(self, config, dag: saldag.Dag,
                 header_flag=True,
                 template_directory="{}/templates/spark".format(os.path.dirname(os.path.realpath(__file__)))):
        super(SparkCodeGen, self).__init__(config, dag)
        self.template_directory = template_directory
        self.header_flag = header_flag

    def _generate_job(self, job_name: str, code_directory: str, op_code: str):
        """ Returns generated Spark code and Job object. """

        template = open(
            "{}/job.tmpl".format(self.template_directory), 'r').read()
        data = {
            'JOB_NAME': job_name,
            'OP_CODE': op_code
        }

        op_code = pystache.render(template, data)

        job = SparkJob(job_name, "{}/{}".format(code_directory, job_name))

        return job, op_code

    def _generate_store(self, op: saldag.OpNode):
        """ Generate code for storing a relation. """

        store_code = ''
        if op.is_leaf():
            template = open(
                "{}/store.tmpl".format(self.template_directory), 'r').read()
            data = {
                'RELATION_NAME': op.out_rel.name,
                'OUTPUT_PATH': self.config.output_path,
            }

            store_code += pystache.render(template, data)

        return store_code

    def _generate_sort_by(self, sort_op: saldag.SortBy):
        """ Generate code for SortBy operations. """

        store_code = self._generate_store(sort_op)

        sort_col = sort_op.sort_by_col.name

        template = open("{0}/{1}.tmpl".format(self.template_directory, 'sort_by'), 'r').read()

        data = {
            'INREL': sort_op.get_in_rel().name,
            'OUTREL': sort_op.out_rel.name,
            'SORT_COL': sort_col,
            'CACHE_VAR': cache_var(sort_op)
        }

        return pystache.render(template, data) + store_code

    def _generate_index(self, index_op: saldag.Index):
        """ Generate code for Index operations. """

        store_code = self._generate_store(index_op)

        template = open(
            "{0}/{1}.tmpl".format(self.template_directory, 'index', 'r')).read()

        data = {
            'INREL': index_op.get_in_rel().name,
            'OUTREL': index_op.out_rel.name,
            'IDX_COL': index_op.idx_col_name,
            'CACHE_VAR': cache_var(index_op)
        }

        return pystache.render(template, data) + store_code

    def _generate_comp_neighs(self, comp_neighs_op: saldag.CompNeighs):

        store_code = self._generate_store(comp_neighs_op)

        template = open(
            "{0}/{1}.tmpl".format(self.template_directory, 'comp_neighs', 'r')).read()

        data = {
            'INREL': comp_neighs_op.get_in_rel().name,
            'OUTREL': comp_neighs_op.out_rel.name,
            'COMP_COL': comp_neighs_op.comp_col.name,
            'CACHE_VAR': cache_var(comp_neighs_op)
        }

        return pystache.render(template, data) + store_code

    def _generate_aggregate(self, agg_op: saldag.Aggregate):
        """ Generate code for Aggregate operations. """

        if agg_op.aggregator == '+':
            aggregator = 'sum'
        else:
            # e.g. - 'max', 'min', 'avg', 'count', 'sum'
            aggregator = agg_op.aggregator

        store_code = self._generate_store(agg_op)

        # codegen can take strings like {'c':'sum', 'd':'sum'}
        aggcol_str = '{' + "'" + agg_op.agg_col.name + "'" + ':' + "'" + aggregator + "'" + '}'

        # TODO: this renaming convention will only work if we stick to general aggregations (sum, min, max, etc.)
        old = aggregator + '(' + agg_op.agg_col.name + ')'
        new = agg_op.out_rel.columns[-1].name

        template = open(
            "{0}/{1}.tmpl".format(self.template_directory, 'agg'), 'r').read()

        data = {
            'GROUPCOLS': ",".join("'" + group_col.name + "'" for group_col in agg_op.group_cols),
            'AGGCOLS': aggcol_str,
            'INREL': agg_op.get_in_rel().name,
            'OUTREL': agg_op.out_rel.name,
            'CACHE_VAR': cache_var(agg_op),
            'OLD': old,
            'NEW': new
        }

        return pystache.render(template, data) + store_code

    def _generate_concat(self, concat_op: saldag.Concat):
        """ Generate code for Concat operations. """

        all_rels = concat_op.get_in_rels()
        test = len(all_rels[0].columns)
        assert (all(test == len(rel.columns) for rel in all_rels))

        store_code = ''
        if concat_op.is_leaf():
            store_code += self._generate_store(concat_op)

        template = open(
            "{0}/{1}.tmpl".format(self.template_directory, 'concat'), 'r').read()

        data = {
            'INRELS': ', '.join(r.name for r in concat_op.get_in_rels()),
            'OUTREL': concat_op.out_rel.name,
            'CACHE_VAR': cache_var(concat_op)
        }

        return pystache.render(template, data) + store_code

    def _generate_create(self, create_op: saldag.Create):

        template = open(
            "{}/create.tmpl".format(self.template_directory), 'r').read()

        schema = ["StructField('{0}', {1}, True)".format(
            col.name, convert_type(col.type_str)) for col in create_op.out_rel.columns]

        data = {
            'RELATION_NAME': create_op.out_rel.name,
            'SCHEMA': 'StructType([' + ','.join(schema) + '])',
            'INPUT_PATH': self.config.input_path + '/' + create_op.out_rel.name + '.csv',
            'CACHE_VAR': cache_var(create_op),
            'HEADER_FLAG': "True" if self.header_flag else "False"
        }

        return pystache.render(template, data)

    def _generate_join(self, join_op: saldag.Join):
        """ Generate code for Join operations. """

        store_code = ''
        if join_op.is_leaf():
            store_code += self._generate_store(join_op)

        # TODO: (ben) should we assume this is always true?
        # (pyspark's join function only takes 1 list of column names as an argument)
        left_names = [col.name for col in join_op.left_join_cols]
        right_names = [col.name for col in join_op.right_join_cols]
        assert (sorted(left_names) == sorted(right_names))
        join_cols = join_op.left_join_cols

        template = open(
            "{0}/{1}.tmpl".format(self.template_directory, 'join'), 'r').read()

        data = {
            'LEFT_PARENT': join_op.get_left_in_rel().name,
            'RIGHT_PARENT': join_op.get_right_in_rel().name,
            'JOIN_COLS': [join_col.name for join_col in join_cols],
            'OUTREL': join_op.out_rel.name,
            'CACHE_VAR': cache_var(join_op)
        }

        return pystache.render(template, data) + store_code

    def _generate_project(self, project_op: saldag.Project):
        """ Generate code for Project operations. """

        store_code = ''
        if project_op.is_leaf():
            store_code += self._generate_store(project_op)

        cols = project_op.selected_cols

        template = open(
            "{0}/{1}.tmpl".format(self.template_directory, 'project'), 'r').read()

        data = {
            'COLS': [c.name for c in cols],
            'INREL': project_op.get_in_rel().name,
            'OUTREL': project_op.out_rel.name,
            'CACHE_VAR': cache_var(project_op)
        }

        return pystache.render(template, data) + store_code

    def _generate_multiply(self, mult_op: saldag.Multiply):
        """ Generate code for Multiply operations. """

        store_code = ''
        if mult_op.is_leaf():
            store_code += self._generate_store(mult_op)

        op_cols = mult_op.operands
        operands = []
        scalar = 1

        for op_col in op_cols:
            if hasattr(op_col, 'name'):
                operands.append(mult_op.get_in_rel().name + '.' + op_col.name)
            else:
                scalar = op_col

        template = open("{0}/{1}.tmpl"
                        .format(self.template_directory, 'multiply'), 'r').read()

        data = {
            'OPERANDS': '*'.join(c for c in operands),
            'SCALAR': scalar,
            'TARGET': mult_op.target_col.name,
            'INREL': mult_op.get_in_rel().name,
            'OUTREL': mult_op.out_rel.name,
            'CACHE_VAR': cache_var(mult_op)
        }

        return pystache.render(template, data) + store_code

    def _generate_divide(self, div_op: saldag.Divide):
        """ Generate code for Divide operations. """

        store_code = ''
        if div_op.is_leaf():
            store_code += self._generate_store(div_op)

        op_cols = div_op.operands
        operands = []
        scalar = 1

        for op_col in op_cols:
            if hasattr(op_col, 'name'):
                operands.append(div_op.get_in_rel().name + '.' + op_col.name)
            else:
                scalar = op_col

        template = open(
            "{0}/{1}.tmpl".format(self.template_directory, 'divide'), 'r').read()

        data = {
            'OPERANDS': '/'.join(c for c in operands),
            'SCALAR': scalar,
            'TARGET': div_op.target_col.name,
            'INREL': div_op.get_in_rel().name,
            'OUTREL': div_op.out_rel.name,
            'TO_INT_CAST': ".cast('integer')" if div_op.target_col.type_str == "INTEGER" else "",
            'CACHE_VAR': cache_var(div_op)
        }

        return pystache.render(template, data) + store_code

    def _generate_distinct(self, distinct_op: saldag.Distinct):
        """ Generate code for Distinct operations. """

        template = open(
            "{}/distinct.tmpl".format(self.template_directory), 'r').read()

        data = {
            'COLS': [c.name for c in distinct_op.selected_cols],
            'OUTREL': distinct_op.out_rel.name,
            'INREL': distinct_op.get_in_rel().name,
            'CACHE_VAR': cache_var(distinct_op)
        }

        return pystache.render(template, data)

    def _write_bash(self, job_name: str):
        """ Generate bash script that runs Spark jobs. """

        roots, leaves = [], []

        nodes = self.dag.top_sort()
        for node in nodes:
            if node.is_root():
                roots.append("{}/{}"
                             .format(self.config.input_path, node.out_rel.name))
            elif node.is_leaf():
                leaves.append("{}/{}"
                              .format(self.config.input_path, node.out_rel.name))

        template = open("{}/bash.tmpl"
                        .format(self.template_directory), 'r').read()

        data = {
            'INPUTS': ' '.join(roots),
            'OUTPUTS': ' '.join(leaves),
            'PATH': "{}/{}".format(self.config.code_path, job_name)
        }

        return pystache.render(template, data)

    def _write_code(self, code: str, job_name: str):
        """ Write generated code to file. """

        os.makedirs("{}/{}".format(self.config.code_path, job_name), exist_ok=True)

        # write code to a file
        pyfile = open("{}/{}/workflow.py".format(self.config.code_path, job_name), 'w')
        pyfile.write(code)

        bash_code = self._write_bash(job_name)
        bash = open("{}/{}/bash.sh".format(self.config.code_path, job_name), 'w')
        bash.write(bash_code)
