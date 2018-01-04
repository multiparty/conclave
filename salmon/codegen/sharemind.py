from salmon.codegen import CodeGen, CodeGenConfig
from salmon.job import SharemindJob
from salmon.dag import *
from salmon.rel import *
import os, sys
import pystache
import shutil


class SharemindCodeGenConfig(CodeGenConfig):

    def __init__(self, job_name=None, home_path="/tmp", use_docker=True, use_hdfs=True):

        super(SharemindCodeGenConfig, self).__init__(job_name)
        self.home_path = home_path
        self.use_docker = use_docker
        self.use_hdfs = use_hdfs


class SharemindCodeGen(CodeGen):

    def __init__(self, config, dag, pid,
                 template_directory="{}/templates/sharemind".format(os.path.dirname(os.path.realpath(__file__)))):

        if not "sharemind" in config.system_configs:
            print("Missing Sharemind configuration in CodeGenConfig!")
            sys.exit(1)
        self.sm_config = config.system_configs['sharemind']

        super(SharemindCodeGen, self).__init__(config, dag)
        self.template_directory = template_directory
        self.pid = pid

    def generate(self, job_name, output_directory):

        job, code = self._generate(job_name, self.config.code_path)
        # store the code in type-specific files
        self._writeCode(code, job_name)
        # return job object
        return job

    def _generate(self, job_name, output_directory):

        # nodes in topological order
        nodes = self.dag.top_sort()

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
            schemas, input_code = self._generate_input_code(
                nodes, job_name, self.config.code_path)
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
            submit_code = self._generate_controller_code(
                nodes, job_name, self.config.code_path)
            op_code["miner"] = miner_code
            op_code["submit"] = submit_code["outer"]
            op_code["submitInner"] = submit_code["inner"]

        # create job
        job = SharemindJob(job_name, self.config.code_path + "/" + job_name,
                           controller_pid, input_parties)
        # check if this party participates in any way
        if not op_code:
            job.skip = True
        return job, op_code

    def _generate_miner_code(self, nodes):

        # TODO: this code should be re-using base class _generate method
        # the code that will run on the miners
        # TODO: handle subclassing more gracefully
        miner_code = ""
        for node in nodes:
            if isinstance(node, IndexAggregate):
                miner_code += self._generateIndexAggregate(node)
            elif isinstance(node, Aggregate):
                miner_code += self._generateAggregate(node)
            elif isinstance(node, Concat):
                miner_code += self._generateConcat(node)
            elif isinstance(node, Create):
                miner_code += self._generateCreate(node)
            elif isinstance(node, Divide):
                miner_code += self._generateDivide(node)
            elif isinstance(node, IndexJoin):
                miner_code += self._generateIndexJoin(node)
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
            elif isinstance(node, Shuffle):
                miner_code += self._generateShuffle(node)
            elif isinstance(node, Persist):
                miner_code += self._generatePersist(node)
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
        # union of all stored_withs gives us all output parties
        output_parties = set().union(
            *[op.out_rel.stored_with for op in open_ops])
        # only support one output party
        assert len(output_parties) == 1, len(output_parties)
        # that output party will be the controller
        return next(iter(output_parties)) # pop

    def _get_input_parties(self, nodes):

        # we need all close ops to get all input parties
        close_ops = filter(lambda op_node: isinstance(op_node, Close), nodes)
        # union of all stored_withs gives us all input parties
        input_parties = set().union(
            *[op.get_in_rel().stored_with for op in close_ops])
        # want these in-order
        return sorted(list(input_parties))

    def _generate_input_code(self, nodes, job_name, output_directory):

        # all schemas of the relations this party will input
        schemas = {}
        # only need close ops to generate schemas
        close_ops = filter(lambda op_node: isinstance(op_node, Close), nodes)
        # only need schemas for my close ops
        my_close_ops = filter(
            lambda close_op: self.pid in close_op.get_in_rel().stored_with, close_ops)
        # all CSVImports this party will perform
        hdfs_import_statements = []
        import_statements = []
        for close_op in my_close_ops:
            # generate schema and get its name
            name, schema, header = self._generateSchema(close_op)
            schemas[name] = schema
            # TODO: hack hack hack
            if self.sm_config.use_hdfs:
                hdfs_import_statements.append(self._generateHDFSImport(
                    close_op, header, job_name)[:-1])
            else:
                hdfs_import_statements.append("cp {} {}".format(
                        self.config.output_path + "/" + name + ".csv",
                        self.config.code_path + "/" + job_name + "/" + name + ".csv"
                    )
                )
            # generate csv import code
            import_statements.append(self._generateCSVImport(
                close_op, output_directory, job_name)[:-1])
        input_code = "&&".join(import_statements)
        # expand top-level
        # TODO: hack hack hack
        if self.sm_config.use_docker:
            top_level_template = open(
                "{0}/csvImportTopLevel.tmpl".format(self.template_directory), 'r').read()
        else:
            top_level_template = open(
                "{0}/csvImportNoDocker.tmpl".format(self.template_directory), 'r').read()
        top_level_data = {
            "SHAREMIND_HOME": self.sm_config.home_path,
            "HDFS_IMPORTS": "\n".join(hdfs_import_statements),
            "IMPORTS": input_code
        }
        # return schemas and input code
        return schemas, pystache.render(top_level_template, top_level_data)

    def _generate_submit_code(self, nodes, job_name, code_path):

        # hack HDFS output
        open_ops = filter(lambda op_node: isinstance(op_node, Open), nodes)
        hdfs_cmds = []
        for open_op in open_ops:
            name = open_op.out_rel.name
            # TODO: hack hack hack
            if self.sm_config.use_hdfs:
                hdfs_cmd = "hadoop fs -put {}.csv {}.csv".format(
                    code_path + "/" + job_name + "/" + name, 
                    self.config.output_path + "/"  + name
                )
            else:
                hdfs_cmd = "cp {}.csv {}.csv".format(
                    code_path + "/" + job_name + "/" + name, 
                    self.config.output_path + "/"  + name
                )    
            hdfs_cmds.append(hdfs_cmd)
        hdfs_cmds_str = "\n".join(hdfs_cmds)

        # code for submitting job to miners
        template = None
        # TODO: hack hack hack
        if self.sm_config.use_docker:
            template = open(
                "{0}/submit.tmpl".format(self.template_directory), 'r').read()
        else:
            template = open(
                "{0}/submitNoDocker.tmpl".format(self.template_directory), 'r').read()
        data = {
            "SHAREMIND_HOME": self.sm_config.home_path,
            "CODE_PATH": code_path + "/" + job_name,
            "HDFS_CMDS": hdfs_cmds_str
        }
        # inner template (separate shell script)
        templateInner = open(
            "{0}/submitInner.tmpl".format(self.template_directory), 'r').read()
        # we need all open ops to get the size of the output rels
        # for parsing
        open_ops = list(filter(lambda op_node: isinstance(op_node, Open), nodes))
        rel_names = [open_op.out_rel.name for open_op in open_ops]
        num_cols = [str(len(open_op.out_rel.columns)) for open_op in open_ops]
        rels_meta_str = " ".join(["--rels-meta {}:{}".format(rname, ncols) for (rname, ncols) in zip(rel_names, num_cols)])

        dataInner = {
            "CODE_PATH": code_path + "/" + job_name,
            "RELS_META": rels_meta_str,
            "LOCAL_OUTPUT_PATH": self.config.code_path + "/" + job_name
        }
        return {
            "outer": pystache.render(template, data),
            "inner": pystache.render(templateInner, dataInner)
        }

    def _generate_controller_code(self, nodes, job_name, output_directory):

        submit_code = self._generate_submit_code(
            nodes, job_name, output_directory)
        return submit_code

    def _generateAggregate(self, agg_op):

        template = open(
            "{0}/aggregateSum.tmpl".format(self.template_directory), 'r').read()

        # for now, only 1 group_col in mpc ops
        # TODO: update template with multi-col
        data = {
            "TYPE": "uint32",
            "OUT_REL_NAME": agg_op.out_rel.name,
            "IN_REL_NAME": agg_op.get_in_rel().name,
            "KEY_COL_IDX": agg_op.group_cols[0].idx,
            "AGG_COL_IDX": agg_op.agg_col.idx
        }
        return pystache.render(template, data)

    def _generateIndexAggregate(self, idx_agg_op):

        template = open(
            "{0}/indexAggregateSum.tmpl".format(self.template_directory), 'r').read()

        data = {
            "TYPE": "uint32",
            "OUT_REL_NAME": idx_agg_op.out_rel.name,
            "IN_REL_NAME": idx_agg_op.get_in_rel().name,
            "GROUP_COL_IDX": idx_agg_op.group_cols[0].idx,
            "AGG_COL_IDX": idx_agg_op.agg_col.idx,
            "EQ_FLAG_REL": idx_agg_op.eq_flag_op.out_rel.name,
            "SORTED_KEYS_REL": idx_agg_op.sorted_keys_op.out_rel.name
        }
        return pystache.render(template, data)


    def _generateClose(self, close_op):

        template = open(
            "{0}/close.tmpl".format(self.template_directory), 'r').read()
        data = {
            "TYPE": "uint32",
            "OUT_REL_NAME": close_op.out_rel.name,
            "IN_REL_NAME": close_op.get_in_rel().name
        }
        return pystache.render(template, data)
        
    def _generateConcat(self, concat_op):

        in_rels = concat_op.get_in_rels()
        assert len(in_rels) > 1

        # Sharemind only allows us to concatenate two relations at a time
        # so we need to chain calls repeatedly for more
        catTemplate = open(
            "{0}/catExpr.tmpl".format(self.template_directory), 'r').read()

        cats = catTemplate
        for in_rel in in_rels[:-2]:
            data = {
                "LEFT_REL": in_rel.name,
                "RIGHT_REL": catTemplate
            }
            cats = pystache.render(cats, data)
        outer = open(
            "{0}/concatDef.tmpl".format(self.template_directory), 'r').read()
        data = {
            "OUT_REL": concat_op.out_rel.name,
            "TYPE": "uint32",
            "CATS": cats
        }
        outer = pystache.render(outer, data)
        data = {
            "LEFT_REL": in_rels[-2].name,
            "RIGHT_REL": in_rels[-1].name
        }
        return pystache.render(outer, data)

    def _generateCreate(self, create_op):

        template = open(
            "{0}/readFromDb.tmpl".format(self.template_directory), 'r').read()
        data = {
            "NAME": create_op.out_rel.name,
            "TYPE": "uint32"
        }
        return pystache.render(template, data)

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
            "TYPE": "uint32",
            "OUT_REL": divide_op.out_rel.name,
            "IN_REL": divide_op.get_in_rel().name,
            "TARGET_COL": divide_op.target_col.idx,
            # hacking array brackets
            "OPERANDS": "{" + operands_str + "}",
            "SCALAR_FLAGS": "{" + scalar_flags_str + "}"
        }
        return pystache.render(template, data)

    def _generateIndexJoin(self, index_join_op):

        template = open(
            "{0}/indexJoin.tmpl".format(self.template_directory), 'r').read()
        left_rel = index_join_op.left_parent.out_rel
        right_rel = index_join_op.right_parent.out_rel
        index_rel = index_join_op.index_rel.out_rel

        data = {
            "TYPE": "uint32",
            "OUT_REL": index_join_op.out_rel.name,
            "LEFT_IN_REL": index_join_op.get_left_in_rel().name,
            "LEFT_KEY_COLS": str(index_join_op.left_join_cols[0].idx),
            "RIGHT_IN_REL": index_join_op.get_right_in_rel().name,
            "RIGHT_KEY_COLS": str(index_join_op.right_join_cols[0].idx),
            "INDEX_REL": index_rel.name
        }
        return pystache.render(template, data)

    def _generateJoin(self, join_op):

        template = open(
            "{0}/join.tmpl".format(self.template_directory), 'r').read()
        left_key_cols_str = ",".join([str(col.idx)
                                      for col in join_op.left_join_cols])
        right_key_cols_str = ",".join(
            [str(col.idx) for col in join_op.right_join_cols])
        left_rel = join_op.left_parent.out_rel
        right_rel = join_op.right_parent.out_rel
        # sharemind adds all columns from right-rel to the result
        # so we need to explicitely exclude these
        cols_to_keep = list(
            range(len(left_rel.columns) + len(right_rel.columns)))
        cols_to_exclude = [col.idx + len(left_rel.columns)
                           for col in join_op.right_join_cols]
        cols_to_keep_str = ",".join(
            [str(idx) for idx in cols_to_keep if idx not in cols_to_exclude])
        data = {
            "TYPE": "uint32",
            "OUT_REL": join_op.out_rel.name,
            "LEFT_IN_REL": join_op.get_left_in_rel().name,
            "LEFT_KEY_COLS": "{" + left_key_cols_str + "}",
            "RIGHT_IN_REL": join_op.get_right_in_rel().name,
            "RIGHT_KEY_COLS": "{" + right_key_cols_str + "}",
            "COLS_TO_KEEP": "{" + cols_to_keep_str + "}"
        }
        return pystache.render(template, data)

    def _generateOpen(self, open_op):

        template = open(
            "{0}/publish.tmpl".format(self.template_directory), 'r').read()
        data = {
            "OUT_REL": open_op.out_rel.name,
            "IN_REL": open_op.get_in_rel().name,
        }
        return pystache.render(template, data)

    def _generateShuffle(self, shuffle_op):

        template = open(
            "{0}/shuffle.tmpl".format(self.template_directory), 'r').read()
        data = {
            "TYPE": "uint32",
            "OUT_REL": shuffle_op.out_rel.name,
            "IN_REL": shuffle_op.get_in_rel().name
        }
        return pystache.render(template, data)

    def _generatePersist(self, persist_op):

        template = open(
            "{0}/persist.tmpl".format(self.template_directory), 'r').read()
        data = {
            "OUT_REL": persist_op.out_rel.name,
            "IN_REL": persist_op.get_in_rel().name,
        }
        return pystache.render(template, data)

    def _generateProject(self, project_op):

        template = open(
            "{0}/project.tmpl".format(self.template_directory), 'r').read()
        selected_cols = project_op.selected_cols
        selectedColStr = ",".join([str(col.idx) for col in selected_cols])
        data = {
            "TYPE": "uint32",
            "OUT_REL": project_op.out_rel.name,
            "IN_REL": project_op.get_in_rel().name,
            # hacking array brackets
            "SELECTED_COLS": "{" + selectedColStr + "}"
        }
        return pystache.render(template, data)

    def _generateMultiply(self, multiply_op):

        template = open(
            "{0}/multiply.tmpl".format(self.template_directory), 'r').read()

        operands = [op.idx if isinstance(
            op, Column) else op for op in multiply_op.operands]
        operands_str = ",".join(str(op) for op in operands)
        scalar_flags = [0 if isinstance(
            op, Column) else 1 for op in multiply_op.operands]
        scalar_flags_str = ",".join(str(op) for op in scalar_flags)

        data = {
            "TYPE": "uint32",
            "OUT_REL": multiply_op.out_rel.name,
            "IN_REL": multiply_op.get_in_rel().name,
            "TARGET_COL": multiply_op.target_col.idx,
            # hacking array brackets
            "OPERANDS": "{" + operands_str + "}",
            "SCALAR_FLAGS": "{" + scalar_flags_str + "}"
        }
        return pystache.render(template, data)

    def _generateSchema(self, close_op):

        in_rel = close_op.get_in_rel()
        in_cols = in_rel.columns
        out_rel = close_op.out_rel
        outCols = out_rel.columns
        colDefs = []
        colDefTemplate = open(
            "{0}/colDef.tmpl".format(self.template_directory), 'r').read()
        for inCol, outCol in zip(in_cols, outCols):
            colData = {
                'IN_NAME': inCol.getName(),
                'OUT_NAME': outCol.getName(),
                'TYPE': "uint32"  # hard-coded for now
            }
            colDefs.append(pystache.render(colDefTemplate, colData))
        colDefStr = "\n".join(colDefs)
        relDefTemplate = open(
            "{0}/relDef.tmpl".format(self.template_directory), 'r').read()
        relData = {
            "NAME": close_op.get_in_rel().name,
            "COL_DEFS": colDefStr
        }
        relDefHeader = ",".join([c.name for c in in_cols])
        relDefStr = pystache.render(relDefTemplate, relData)
        return in_rel.name, relDefStr, relDefHeader

    def _generateHDFSImport(self, close_op, header, job_name):

        template = open(
            "{0}/hdfsImport.tmpl".format(self.template_directory), 'r').read()
        data = {
            "IN_NAME": close_op.get_in_rel().name,
            "SCHEMA_HEADER": header,
            "INPUT_PATH": self.config.input_path,
            "CODE_PATH": self.config.code_path + "/" + job_name,
        }
        return pystache.render(template, data)

    def _generateCSVImport(self, close_op, output_directory, job_name):

        def _delim_lookup(delim):

            if delim == ",":
                return "c"
            else:
                raise Exception("unknown delimiter")

        template = open(
            "{0}/csvImport.tmpl".format(self.template_directory), 'r').read()
        data = {
            "IN_NAME": close_op.get_in_rel().name,
            "INPUT_PATH": self.config.input_path,
            "CODE_PATH": self.config.code_path + "/" + job_name,
            'DELIMITER': _delim_lookup(self.config.delimiter),
        }
        return pystache.render(template, data)

    def _writeCode(self, code_dict, job_name):

        def _write(root_dir, fn, ext, content, job_name):

            fullpath = "{}/{}/{}.{}".format(root_dir, job_name, fn, ext)
            os.makedirs(os.path.dirname(fullpath), exist_ok=True)
            with open(fullpath, "w") as f:
                f.write(content)

        ext_lookup = {
            "schemas": "xml",
            "input": "sh",
            "submit": "sh",
            "submitInner": "sh",
            "miner": "sc"
        }

        # write files
        job_code_path = self.config.code_path
        for code_type, code in code_dict.items():
            if code_type == "schemas":
                schemas = code
                for schema_name in schemas:
                    _write(job_code_path, schema_name,
                           ext_lookup[code_type], schemas[schema_name], job_name)
            else:
                _write(job_code_path, code_type, ext_lookup[code_type], code, job_name)
