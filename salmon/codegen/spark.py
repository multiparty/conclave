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


    # TODO: (ben) only agg_+.tmpl is updated for multiple group cols right now
    def _generateAggregate(self, agg_op):

        store_code = self._generateStore(agg_op)

        agg_type = 'agg_' + agg_op.aggregator

        template = open("{0}/{1}.tmpl"
                        .format(self.template_directory, agg_type), 'r').read()

        data = {
            'GROUPCOL_IDS': [groupCol.idx for groupCol in agg_op.groupCols],
            'AGGCOL_IDS': [agg_op.aggCol.idx],
            'INREL': agg_op.getInRel().name,
            'OUTREL': agg_op.outRel.name,
            'CACHE_VAR': cache_var(agg_op)
        }

        return pystache.render(template, data) + store_code

    def _generateConcat(self, concat_op):

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

    # TODO: create.tmpl assumes the rows are tab-delimited right now
    def _generateCreate(self, create_op):

        template = open("{}/create.tmpl"
                        .format(self.template_directory), 'r').read()

        data = {
            'RELATION_NAME': create_op.outRel.name,
            'INPUT_PATH': self.config.input_path,
            'DELIMITER': self.config.delimiter,
            'CACHE_VAR': cache_var(create_op)
        }

        return pystache.render(template, data)

    def _generateJoin(self, join_op):

        store_code = ''
        if join_op.isLeaf():
            store_code += self._generateStore(join_op)

        leftName = join_op.getLeftInRel().name
        rightName = join_op.getRightInRel().name

        leftJoinCols, rightJoinCols = join_op.leftJoinCols, join_op.rightJoinCols

        template = open("{0}/{1}.tmpl"
                        .format(self.template_directory, 'join'), 'r').read()

        data = {
            'LEFT_PARENT': leftName,
            'RIGHT_PARENT': rightName,
            'LEFT_COLS': [leftJoinCol.idx for leftJoinCol in leftJoinCols],
            'RIGHT_COLS': [rightJoinCol.idx for rightJoinCol in rightJoinCols],
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
            'COL_IDS': [c.idx for c in cols],
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
        targetCol = mult_op.targetCol
        operands = []
        scalar = 1

        if targetCol.name == op_cols[0].name:
            new_col = False
            # targetCol is at op_cols[0]
            for op_col in op_cols:
                if hasattr(op_col, 'idx'):
                    if op_col.idx != targetCol.idx:
                        operands.append(op_col.idx)
                else:
                    scalar = op_col
        else:
            new_col = True
            for op_col in op_cols:
                if hasattr(op_col, 'idx'):
                    operands.append(op_col.idx)
                else:
                    scalar = op_col

        template = open("{0}/{1}.tmpl"
                        .format(self.template_directory, 'multiply'), 'r').read()

        data = {
            'NEWCOL_FLAG': new_col,
            'OPERANDS': [idx for idx in operands],
            'SCALAR': scalar,
            'TARGET_ID': targetCol.idx,
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
        targetCol = div_op.targetCol
        operands = []
        scalar = 1

        if targetCol.name == op_cols[0].name:
            new_col = False
            # targetCol is at op_cols[0]
            for op_col in op_cols:
                if hasattr(op_col, 'idx'):
                    if op_col.idx != targetCol.idx:
                        operands.append(op_col.idx)
                else:
                    scalar = op_col
        else:
            new_col = True
            for op_col in op_cols:
                if hasattr(op_col, 'idx'):
                    operands.append(op_col.idx)
                else:
                    scalar = op_col

        template = open("{0}/{1}.tmpl"
                        .format(self.template_directory, 'divide'), 'r').read()

        data = {
            'NEWCOL_FLAG': new_col,
            'OPERANDS': [idx for idx in operands],
            'SCALAR': scalar,
            'TARGET_ID': div_op.targetCol.idx,
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

    # TODO: (ben) only supports single column operation 
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
                roots.append("{}/{}.csv"
                             .format(self.config.input_path, node.outRel.name))
            elif node.isLeaf():
                leaves.append("{}/{}.csv"
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
