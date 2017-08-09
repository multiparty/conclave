from salmon.codegen import CodeGen
from salmon.job import SharemindJob
from salmon.dag import *
from salmon.rel import *
import os
import pystache
import shutil

class SharemindCodeGen(CodeGen):

    def __init__(self, dag, pid, template_directory="{}/templates/sharemind".format(os.path.dirname(os.path.realpath(__file__)))):

        super(SharemindCodeGen, self).__init__(dag)
        self.template_directory = template_directory
        self.pid = pid

    def generate(self, job_name, output_directory):

        job, code = self._generate(job_name, output_directory)
        # store the code in type-specific files
        self._writeCode(code, output_directory, job_name)
        # return job object
        return job

    def _generate(self, job_name, output_directory):

        # nodes in topological order
        nodes = self.dag.topSort()

        # determine party that will act as the controller
        controller_pid = self._get_controller_pid(nodes)
        # determine all input parties
        input_parties = self._get_input_parties(nodes)

        # dict of all generated code
        op_code = {}

        # generate input code and schemas for input
        if self.pid in input_parties:
            # the data model definitions used by CSVImporter
            # and the code the party will run to secret-share its
            # inputs with the miners
            schemas, input_code = self._generate_input_code(nodes)
            op_code["schemas"] = schemas
            op_code["input"] = input_code

        # if we are the controller we need to generate miner
        # code and controller code
        if self.pid == controller_pid:
            # the code the miners will run after the data has been
            # secret shared
            miner_code = self._generate_miner_code(nodes)
            # code to submit the job and receive the output
            # (currently assumes there is only one output party)
            controller_code = self._generate_controller_code(nodes)
            op_code["miner"] = miner_code
            op_code["controller"] = controller_code

        # sanity check
        assert op_code

        # create job
        job = SharemindJob(job_name, controller_pid, input_parties)
        return job, op_code

    def _generate_miner_code(self, nodes):

        # the code that will run on the miners
        miner_code = ""
        for node in nodes:
            if isinstance(node, Aggregate):
                miner_code += self._generateAggregate(node)
            elif isinstance(node, Concat):
                miner_code += self._generateConcat(node)
            elif isinstance(node, Divide):
                miner_code += self._generateDivide(node)
            elif isinstance(node, Join):
                miner_code += self._generateJoin(node)
            elif isinstance(node, Multiply):
                miner_code += self._generateMultiply(node)
            elif isinstance(node, Open):
                miner_code += self._generateOpen(node)
            elif isinstance(node, Project):
                miner_code += self._generateProject(node)
            elif isinstance(node, Close):
                miner_code += self._generateClose(node)
            else:
                print("encountered unknown operator type", repr(node))

        # expand top-level protocol template
        template = open(
            "{0}/protocol.tmpl".format(self.template_directory), 'r').read()
        return pystache.render(
            template, {"PROTOCOL_CODE": miner_code})

    def _get_controller_pid(self, nodes):

        # we need all open ops to get all output parties
        open_ops = filter(lambda op_node: isinstance(op_node, Open), nodes)
        # union of all storedWiths gives us all output parties
        output_parties = set().union(
            *[op.outRel.storedWith for op in open_ops])
        # only support one output party
        assert(len(output_parties) == 1)
        # that output party will be the controller
        return next(iter(output_parties))  # pop

    def _get_input_parties(self, nodes):

        # we need all close ops to get all input parties
        close_ops = filter(lambda op_node: isinstance(op_node, Close), nodes)
        # union of all storedWiths gives us all input parties
        input_parties = set().union(
            *[op.getInRel().storedWith for op in close_ops])
        # want these in-order
        return sorted(list(input_parties))

    def _generate_input_code(self, nodes):

        # all schemas of the relations this party will input
        schemas = {}
        # all CSVImports this party will perform
        input_code = ""
        # only need close ops to generate schemas
        close_ops = filter(lambda op_node: isinstance(op_node, Close), nodes)
        # only need schemas for my close ops
        my_close_ops = filter(
            lambda close_op: self.pid in close_op.getInRel().storedWith, close_ops)
        for close_op in my_close_ops:
            # generate schema and get its name
            name, schema = self._generateSchema(close_op)
            schemas[name] = schema
            # generate csv import code
            input_code += self._generateCSVImport(close_op)
        # return schemas and input code
        return schemas, input_code

    def _generate_controller_code(self, nodes):

        # TODO: only a stub
        return "sm_compile_and_run.sh protocol.sc"

    def _generateJob(self, job_name, output_parties, op_code):

        return op_code

    def _generateAggregate(self, agg_op):

        template = open(
            "{0}/aggregateSum.tmpl".format(self.template_directory), 'r').read()

        # for now, only 1 groupCol in mpc ops
        data = {
            "TYPE": "xor_uint32",
            "OUT_REL_NAME": agg_op.outRel.name,
            "IN_REL_NAME": agg_op.getInRel().name,
            "KEY_COL_IDX": agg_op.groupCols[0].idx,
            "AGG_COL_IDX": agg_op.aggCol.idx
        }
        return pystache.render(template, data)

    def _generateClose(self, close_op):

        template = open(
            "{0}/readFromDb.tmpl".format(self.template_directory), 'r').read()
        data = {
            "NAME": close_op.outRel.name,
            "TYPE": "xor_uint32"
        }
        return pystache.render(template, data)

    def _generateConcat(self, concat_op):

        inRels = concat_op.getInRels()
        assert len(inRels) > 1

        # Sharemind only allows us to concatenate two relations at a time
        # so we need to chain calls repeatedly for more
        catTemplate = open(
            "{0}/catExpr.tmpl".format(self.template_directory), 'r').read()

        cats = catTemplate
        for inRel in inRels[:-2]:
            data = {
                "LEFT_REL": inRel.name,
                "RIGHT_REL": catTemplate
            }
            cats = pystache.render(cats, data)
        outer = open(
            "{0}/concatDef.tmpl".format(self.template_directory), 'r').read()
        data = {
            "OUT_REL": concat_op.outRel.name,
            "TYPE": "xor_uint32",
            "CATS": cats
        }
        outer = pystache.render(outer, data)
        data = {
            "LEFT_REL": inRels[-2].name,
            "RIGHT_REL": inRels[-1].name
        }
        return pystache.render(outer, data)

    def _generateCreate(self, create_op):

        raise Exception("Create operator encountered during Sharemind codegen")

    def _generateDivide(self, divide_op):

        template = open(
            "{0}/divide.tmpl".format(self.template_directory), 'r').read()

        operands = [op.idx if isinstance(
            op, Column) else op for op in divide_op.operands]
        operands_str = ",".join(str(op) for op in operands)
        scalar_flags = [0 if isinstance(
            op, Column) else 1 for op in divide_op.operands]
        scalar_flags_str = ",".join(str(op) for op in scalar_flags)

        data = {
            "TYPE": "xor_uint32",
            "OUT_REL": divide_op.outRel.name,
            "IN_REL": divide_op.getInRel().name,
            "TARGET_COL": divide_op.targetCol.idx,
            # hacking array brackets
            "OPERANDS": "{" + operands_str + "}",
            "SCALAR_FLAGS": "{" + scalar_flags_str + "}"
        }
        return pystache.render(template, data)

    def _generateJoin(self, join_op):

        template = open(
            "{0}/join.tmpl".format(self.template_directory), 'r').read()
        data = {
            "TYPE": "xor_uint32",
            "OUT_REL": join_op.outRel.name,
            "LEFT_IN_REL": join_op.getLeftInRel().name,
            "LEFT_KEY_COL": join_op.leftJoinCol.idx,
            "RIGHT_IN_REL": join_op.getRightInRel().name,
            "RIGHT_KEY_COL": join_op.rightJoinCol.idx
        }
        return pystache.render(template, data)

    def _generateOpen(self, open_op):

        template = open(
            "{0}/publish.tmpl".format(self.template_directory), 'r').read()
        data = {
            "OUT_REL": open_op.outRel.name,
            "IN_REL": open_op.getInRel().name,
        }
        return pystache.render(template, data)

    def _generateProject(self, project_op):

        template = open(
            "{0}/project.tmpl".format(self.template_directory), 'r').read()
        selectedCols = project_op.selectedCols
        selectedColStr = ",".join([str(col.idx) for col in selectedCols])
        data = {
            "TYPE": "xor_uint32",
            "OUT_REL": project_op.outRel.name,
            "IN_REL": project_op.getInRel().name,
            # hacking array brackets
            "SELECTED_COLS": "{" + selectedColStr + "}"
        }
        return pystache.render(template, data)

    def _generateMultiply(self, multiply_op):

        template = open(
            "{0}/multiply.tmpl".format(self.template_directory), 'r').read()

        operands = multiply_op.operands
        col_op_indeces = [col.idx for col in filter(
            lambda col: isinstance(col, Column), operands)]
        col_op_str = ",".join([str(col) for col in col_op_indeces])
        scalar_ops = list(
            filter(lambda col: not isinstance(col, Column), operands))
        scalar_ops_str = ",".join([str(scalar) for scalar in scalar_ops])

        data = {
            "TYPE": "xor_uint32",
            "OUT_REL": multiply_op.outRel.name,
            "IN_REL": multiply_op.getInRel().name,
            "TARGET_COL": multiply_op.targetCol.idx,
            # hacking array brackets
            "COL_OP_INDECES": "{" + col_op_str + "}",
            "SCALAR_OPS": "{" + scalar_ops_str + "}"
        }
        return pystache.render(template, data)

    def _generateSchema(self, close_op):

        inRel = close_op.getInRel()
        inCols = inRel.columns
        outRel = close_op.outRel
        outCols = outRel.columns
        colDefs = []
        colDefTemplate = open(
            "{0}/colDef.tmpl".format(self.template_directory), 'r').read()
        for inCol, outCol in zip(inCols, outCols):
            colData = {
                'IN_NAME': inCol.getName(),
                'OUT_NAME': outCol.getName(),
                'TYPE': "xor_uint32"  # hard-coded for now
            }
            colDefs.append(pystache.render(colDefTemplate, colData))
        colDefStr = "\n".join(colDefs)
        relDefTemplate = open(
            "{0}/relDef.tmpl".format(self.template_directory), 'r').read()
        relData = {
            "NAME": outRel.name,
            "COL_DEFS": colDefStr
        }
        relDefStr = pystache.render(relDefTemplate, relData)
        return inRel.name, relDefStr

    def _generateCSVImport(self, close_op):

        template = open(
            "{0}/csvImport.tmpl".format(self.template_directory), 'r').read()
        data = {
            "IN_NAME": close_op.getInRel().name
        }
        return pystache.render(template, data)

    def _writeCode(self, code_dict, output_directory, job_name):

        def _write(root_dir, fn, ext, content):
            fullpath = "{}/{}.{}".format(root_dir, fn, ext)
            os.makedirs(os.path.dirname(fullpath), exist_ok=True)
            with open(fullpath, "w") as f:
                f.write(content)

        ext_lookup = {
            "schemas": "xml",
            "input": "sh",
            "controller": "sh",
            "miner": "sc"
        }

        # root directory to write to
        job_root_dir = "{}/{}".format(output_directory, job_name)
        # get rid of old files
        try:
            shutil.rmtree(job_root_dir)
        except FileNotFoundError:
            pass
        # write files
        for code_type, code in code_dict.items():
            if code_type == "schemas":
                schemas = code
                for schema_name in schemas:
                    _write(job_root_dir, schema_name,
                           ext_lookup[code_type], schemas[schema_name])
            else:
                _write(job_root_dir, code_type, ext_lookup[code_type], code)
