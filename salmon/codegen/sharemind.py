from salmon.codegen import CodeGen
from salmon.dag import *
from salmon.rel import *
import os
import pystache



class SharemindCodeGen(CodeGen):
    # General problem with Sharemind codegen:
    # Sharemind provides the abstraction of a cluster.
    # One party submits the job to the miners instead
    # of the miners each executing the code. In addition
    # the code is returned to one party only (the party
    # that submitted the code to begin with)

    def __init__(self, dag, template_directory="{}/templates/sharemind".format(os.path.dirname(os.path.realpath(__file__)))):

        super(SharemindCodeGen, self).__init__(dag)
        self.template_directory = template_directory

    def _generate(self, job_name, output_directory):
        # TODO: think of a way to extend CodeGen._generate
        # instead of entirely rewriting it

        # generate code for the DAG stored. overwriting
        # method since we have multiple code objects being
        # create, instead of one

        # the data model definitions used by CSVImporter
        # there will be one per store operation
        schemas = {}
        # the code the parties will run to secret-share their
        # inputs with the miners
        input_code = ""
        # the code the miners will run after the data has been
        # secret shared
        miner_code = ""
        # code to submit the job and receive the output
        # (currently assumes there is only one output party)
        controller_code = ""

        # topological traversal
        nodes = self.dag.topSort()
        # for each op
        for node in nodes:
            if isinstance(node, Aggregate):
                miner_code += self._generateAggregate(node)
            elif isinstance(node, Concat):
                miner_code += self._generateConcat(node)
            elif isinstance(node, Join):
                miner_code += self._generateJoin(node)
            elif isinstance(node, Multiply):
                miner_code += self._generateMultiply(node)
            elif isinstance(node, Open):
                # open op needs adds miner code and controller code
                # for receiving results
                controller_open_code, miner_open_code = self._generateOpen(node)
                controller_code += controller_open_code
                miner_code += miner_open_code
            elif isinstance(node, Project):
                miner_code += self._generateProject(node)
            elif isinstance(node, Close):
                # the store operation adds to the input task since we
                # need to secret share, as well as to the protocol task
                schemaName, schema, in_code, prot_code = self._generateClose(
                    node)
                schemas[schemaName] = schema
                input_code += in_code
                miner_code += prot_code
            else:
                print("encountered unknown operator type", repr(node))

        topLevelProtTemplate = open(
            "{0}/protocol.tmpl".format(self.template_directory), 'r').read()
        expandedMinerCode = pystache.render(
            topLevelProtTemplate, {"PROTOCOL_CODE": miner_code})

        op_code = {
            "schemas": schemas,
            "input": input_code,
            "protocol": expandedMinerCode,
            "controller": controller_code
        }
        # expand top-level job template and return code
        return self._generateJob(job_name, op_code)

    def _generateJob(self, job_name, op_code):

        return op_code

    def _generateAggregate(self, agg_op):

        template = open(
            "{0}/aggregateSum.tmpl".format(self.template_directory), 'r').read()
        data = {
            "TYPE": "xor_uint32",
            "OUT_REL_NAME": agg_op.outRel.name,
            "IN_REL_NAME": agg_op.getInRel().name,
            "KEY_COL_IDX": agg_op.keyCol.idx,
            "AGG_COL_IDX": agg_op.aggCol.idx
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

        def _toMiner(open_op):
            # miner portion of open op
            template = open(
                "{0}/publish.tmpl".format(self.template_directory), 'r').read()
            data = {
                "OUT_REL": open_op.outRel.name,
                "IN_REL": open_op.getInRel().name,
            }
            return pystache.render(template, data)

        def _toController(open_op):
            # TODO: controller portion of handling output
            return ""

        return _toController(open_op), _toMiner(open_op)

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
        col_op_indeces = [col.idx for col in filter(lambda col: isinstance(col, Column), operands)]
        col_op_str = ",".join([str(col) for col in col_op_indeces])
        scalar_ops = list(filter(lambda col: not isinstance(col, Column), operands))
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

    def _generateClose(self, store_op):

        def _toSchema(store_op):

            inRel = store_op.getInRel()
            inCols = inRel.columns
            outRel = store_op.outRel
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
            return relDefStr

        def _toCSVImp(store_op):

            template = open(
                "{0}/csvImport.tmpl".format(self.template_directory), 'r').read()
            data = {
                "HDFS_ROOT": "root", # hard-coded for now
                "IN_NAME": store_op.getInRel().name
            }
            return pystache.render(template, data)

        def _toProtocol(store_op):

            template = open(
                "{0}/readFromDb.tmpl".format(self.template_directory), 'r').read()
            data = {
                "NAME": store_op.outRel.name,
                "TYPE": "xor_uint32"
            }
            return pystache.render(template, data)

        res = (
            store_op.getInRel().name,
            _toSchema(store_op),
            _toCSVImp(store_op),
            _toProtocol(store_op)
        )
        return res
