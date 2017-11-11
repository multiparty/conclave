from salmon.job import SparkJob
from salmon.codegen import CodeGen
import os, pystache


def cache_var(op_node):
    if len(op_node.children) > 1:
        return ".cache()"
    else:
        return ''


class SparkCodeGen(CodeGen):

    def __init__(self, config, dag,
            template_directory="{}/templates/spark".format(os.path.dirname(os.path.realpath(__file__)))):
        super(SparkCodeGen, self).__init__(config, dag)
        self.template_directory = template_directory

    def _generateJob(self, job_name, code_directory, op_code):

        template = open("{}/job.tmpl"
                        .format(self.template_directory), 'r').read()
        data = {
            'JOB_NAME': job_name,
            'OP_CODE': op_code
        }

        op_code = pystache.render(template, data)

        job = SparkJob(job_name, "{}/{}".format(code_directory, job_name))

        return job, op_code

    def _generateStore(self, op):

        store_code = ''
        if op.isLeaf():
            template = open("{}/store.tmpl"
                            .format(self.template_directory), 'r').read()
            data = {
                'RELATION_NAME': op.outRel.name,
                'OUTPUT_PATH': self.config.output_path,
            }
            store_code += pystache.render(template, data)

        return store_code

    # TODO: (ben) find way to do this without converting to RDD first
    # (monotonically_increasing_id doesn't give sequential indices)
    def _generateIndex(self, index_op):

        store_code = self._generateStore(index_op)

        template = open("{0}/{1}.tmpl"
                        .format(self.template_directory, 'index'), 'r').read()

        data = {
            'INREL': index_op.getInRel().name,
            'OUTREL': index_op.outRel.name,
            'CACHE_VAR': cache_var(index_op)
        }

        return pystache.render(template, data) + store_code

    def _generateAggregate(self, agg_op):

        # TODO: (ben) ask about switching sum aggregator in scripts from '+' to 'sum'
        if agg_op.aggregator == '+':
            aggregator = 'sum'
        else:
            # e.g. - 'max', 'min', 'avg', 'count'
            aggregator = agg_op.aggregator

        store_code = self._generateStore(agg_op)

        # TODO: (ben) will only have to modify this if we want multiple aggcols in future
        # codegen can take strings like {'c':'sum', 'd':'sum'}
        aggcol_str = '{' + agg_op.aggCol.name + ':' + aggregator + '}'

        template = open("{0}.tmpl"
                        .format(self.template_directory), 'r').read()

        data = {
            'GROUPCOL_IDS': ",".join(groupCol.name for groupCol in agg_op.groupCols),
            'AGGCOL_IDS': aggcol_str,
            'INREL': agg_op.getInRel().name,
            'OUTREL': agg_op.outRel.name,
            'CACHE_VAR': cache_var(agg_op)
        }

        return pystache.render(template, data) + store_code

    def _generateConcat(self, concat_op):

        all_rels = concat_op.getInRels
        test = len(all_rels[0].columns)
        assert(all(test == len(rel.columns) for rel in all_rels))

        store_code = ''
        if concat_op.isLeaf():
            store_code += self._generateStore(concat_op)

        template = open("{0}/{1}.tmpl"
                        .format(self.template_directory, 'concat'), 'r').read()

        data = {
            'INRELS': [r.name for r in concat_op.getInRels()],
            'OUTREL': concat_op.outRel.name,
            'CACHE_VAR': cache_var(concat_op)
        }

        return pystache.render(template, data) + store_code

    # TODO: uses 6 partitions, make configurable in future
    def _generateCreate(self, create_op):

        template = open("{}/create.tmpl"
                        .format(self.template_directory), 'r').read()

        data = {
            'RELATION_NAME': create_op.outRel.name,
            'INPUT_PATH': self.config.input_path,
            'CACHE_VAR': cache_var(create_op)
        }

        return pystache.render(template, data)

    def _generateJoin(self, join_op):

        store_code = ''
        if join_op.isLeaf():
            store_code += self._generateStore(join_op)

        # TODO: (ben) should we assume this is always true?
        # (pyspark's join function only takes 1 list of column names as an argument)
        assert(sorted(join_op.leftJoinCols) == sorted(join_op.rightJoinCols))
        join_cols = join_op.leftJoinCols

        template = open("{0}/{1}.tmpl"
                        .format(self.template_directory, 'join'), 'r').read()

        data = {
            'LEFT_PARENT': join_op.getLeftInRel().name,
            'RIGHT_PARENT': join_op.getRightInRel().name,
            'JOIN_COLS': [join_col.name for join_col in join_cols],
            'OUTREL': join_op.outRel.name,
            'CACHE_VAR': cache_var(join_op)
        }

        return pystache.render(template, data) + store_code

    def _generateProject(self, project_op):

        store_code = ''
        if project_op.isLeaf():
            store_code += self._generateStore(project_op)

        cols = project_op.selectedCols

        template = open("{0}/{1}.tmpl"
                        .format(self.template_directory, 'project'), 'r').read()

        data = {
            'COLS': [c.name for c in cols],
            'INREL': project_op.getInRel().name,
            'OUTREL': project_op.outRel.name,
            'CACHE_VAR': cache_var(project_op)
        }
        return pystache.render(template, data) + store_code

    def _generateMultiply(self, mult_op):

        store_code = ''
        if mult_op.isLeaf():
            store_code += self._generateStore(mult_op)

        op_cols = mult_op.operands
        operands = []
        scalar = 1

        for op_col in op_cols:
            if hasattr(op_col, 'name'):
                operands.append(op_col.name)
            else:
                scalar = op_col

        template = open("{0}/{1}.tmpl"
                        .format(self.template_directory, 'multiply'), 'r').read()

        data = {
            'OPERANDS': '*'.join(c for c in operands),
            'SCALAR': scalar,
            'TARGET': mult_op.targetCol.name,
            'INREL': mult_op.getInRel().name,
            'OUTREL': mult_op.outRel.name,
            'CACHE_VAR': cache_var(mult_op)
        }

        return pystache.render(template, data) + store_code

    def _generateDivide(self, div_op):

        store_code = ''
        if div_op.isLeaf():
            store_code += self._generateStore(div_op)

        op_cols = div_op.operands
        operands = []
        scalar = 1

        for op_col in op_cols:
            if hasattr(op_col, 'name'):
                operands.append(op_col.name)
            else:
                scalar = op_col

        template = open("{0}/{1}.tmpl"
                        .format(self.template_directory, 'divide'), 'r').read()

        data = {
            'OPERANDS': '*'.join(c for c in operands),
            'SCALAR': scalar,
            'TARGET': div_op.targetCol.name,
            'INREL': div_op.getInRel().name,
            'OUTREL': div_op.outRel.name,
            'CACHE_VAR': cache_var(div_op)
        }

        return pystache.render(template, data) + store_code

    def _generateStore(self, op):

        template = open("{}/store.tmpl"
                        .format(self.template_directory), 'r').read()
        data = {
            'RELATION_NAME': op.outRel.name,
            'DELIMITER': self.config.delimiter,
            'OUTPUT_PATH': self.config.output_path,
        }

        return pystache.render(template, data)

    def _generateDistinct(self, op):

        template = open("{}/distinct.tmpl"
                        .format(self.template_directory), 'r').read()

        data = {
            'OUTREL': op.outRel.name,
            'INREL': op.getInRel().name,
            'CACHE_VAR': cache_var(op)
        }

        return pystache.render(template, data)

    def _writeBash(self, job_name):
        roots, leaves = [], []

        nodes = self.dag.topSort()
        for node in nodes:
            if node.isRoot():
                roots.append("{}/{}"
                             .format(self.config.input_path, node.outRel.name))
            elif node.isLeaf():
                leaves.append("{}/{}"
                              .format(self.config.input_path, node.outRel.name))

        template = open("{}/bash.tmpl"
                        .format(self.template_directory), 'r').read()

        data = {
            'INPUTS': ' '.join(roots),
            'OUTPUTS': ' '.join(leaves),
            'PATH': "{}/{}".format(self.config.code_path, job_name)
        }

        return pystache.render(template, data)

    def _writeCode(self, code, job_name):

        os.makedirs("{}/{}".format(self.config.code_path, job_name), exist_ok=True)

        # write code to a file
        pyfile = open("{}/{}/workflow.py".format(self.config.code_path, job_name), 'w')
        pyfile.write(code)

        bash_code = self._writeBash(job_name)
        bash = open("{}/{}/bash.sh".format(self.config.code_path, job_name), 'w')
        bash.write(bash_code)
