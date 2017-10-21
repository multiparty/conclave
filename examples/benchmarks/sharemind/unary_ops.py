from salmon.codegen import CodeGenConfig
from salmon.codegen.sharemind import SharemindCodeGen, SharemindCodeGenConfig
import salmon.dispatch
import salmon.net
from salmon.comp import dagonly
import salmon.lang as sal
from salmon.utils import *
import sys
from random import shuffle


def setup():

    # define inputs
    colsIn1 = [
        defCol("a", "INTEGER", [1]),
        defCol("b", "INTEGER", [1])
    ]
    colsIn2 = [
        defCol("a", "INTEGER", [2]),
        defCol("b", "INTEGER", [2])
    ]
    colsIn3 = [
        defCol("a", "INTEGER", [3]),
        defCol("b", "INTEGER", [3])
    ]

    in1 = sal.create("in1", colsIn1, set([1]))
    in2 = sal.create("in2", colsIn2, set([2]))
    in3 = sal.create("in3", colsIn3, set([3]))

    cl1 = sal._close(in1, "cl1", set([1, 2, 3]))
    cl2 = sal._close(in2, "cl2", set([1, 2, 3]))
    cl3 = sal._close(in3, "cl3", set([1, 2, 3]))

    rel = sal.concat([cl1, cl2, cl3], "rel")

    return set([in1, in2, in3]), rel


@dagonly
def agg(pid, config, sharemind_peer, f_size):

    def protocol():

        inputs, rel = setup()
        res = sal.aggregate(rel, "agg", ["a"], "b", "+", "total")

        opened = sal._open(res, "opened", 1)

        return inputs

    cg = SharemindCodeGen(config, protocol(), pid)
    job = cg.generate("agg_{}".format(f_size) + str(pid), sharemind_home)
    job_queue = [job]

    salmon.dispatch.dispatch_all(None, sharemind_peer, job_queue)


@dagonly
def col_div(pid, config, sharemind_peer, f_size):

    def protocol():

        inputs, rel = setup()
        res = sal.divide(rel, 'div1', 'a', ['a', 'b'])

        opened = sal._open(res, "opened", 1)

        return inputs

    cg = SharemindCodeGen(config, protocol(), pid)
    job = cg.generate("col_div_{}".format(f_size) + str(pid), sharemind_home)
    job_queue = [job]

    salmon.dispatch.dispatch_all(None, sharemind_peer, job_queue)


@dagonly
def col_mult(pid, config, sharemind_peer, f_size):

    def protocol():

        inputs, rel = setup()
        res = sal.multiply(rel, 'mult1', 'a', ['a', 'b'])

        opened = sal._open(res, "opened", 1)

        return inputs

    cg = SharemindCodeGen(config, protocol(), pid)
    job = cg.generate("col_mult_{}".format(f_size) + str(pid), sharemind_home)
    job_queue = [job]

    salmon.dispatch.dispatch_all(None, sharemind_peer, job_queue)


@dagonly
def project(pid, config, sharemind_peer, f_size):

    def protocol():

        inputs, rel = setup()

        cols = ([column.name for column in rel.outRel.columns])
        shuffle(cols)

        res = sal.project(rel, "proja", cols)

        opened = sal._open(res, "opened", 1)

    cg = SharemindCodeGen(config, protocol(), pid)
    job = cg.generate("project_{}".format(f_size) + str(pid), sharemind_home)
    job_queue = [job]

    salmon.dispatch.dispatch_all(None, sharemind_peer, job_queue)


if __name__ == "__main__":

    pid = sys.argv[1]
    hdfs_namenode = sys.argv[2]
    hdfs_root = sys.argv[3]
    # configurable benchmark size
    filesize = sys.argv[4]
    op = sys.argv[5]

    sharemind_home = "/home/sharemind/Sharemind-SDK/sharemind/client"

    workflow_name = "{}_{}_{}".format(op, filesize, pid)
    sm_cg_config = SharemindCodeGenConfig(workflow_name, "/mnt/shared")
    codegen_config = CodeGenConfig(
        workflow_name).with_sharemind_config(sm_cg_config)
    codegen_config.code_path = "/mnt/shared/" + workflow_name
    codegen_config.input_path = "hdfs://{}/{}/{}"\
        .format(hdfs_namenode, hdfs_root, filesize)
    codegen_config.output_path = "hdfs://{}/{}/{}_{}"\
        .format(hdfs_namenode, hdfs_root, op, filesize)
    codegen_config.pid = pid
    codegen_config.name = workflow_name

    sharemind_config = {
        "pid": pid,
        "parties": {
            1: {"host": "ca-spark-node-0", "port": 9001},
            2: {"host": "cb-spark-node-0", "port": 9002},
            3: {"host": "cc-spark-node-0", "port": 9003}
        }
    }
    sm_peer = salmon.net.setup_peer(sharemind_config)

    codegen_config = CodeGenConfig()

    if op == 'agg':
        agg(pid, codegen_config, sm_peer, filesize)
    elif op == 'col_div':
        col_div(pid, codegen_config, sm_peer, filesize)
    elif op == 'col_mult':
        col_mult(pid, codegen_config, sm_peer, filesize)
    elif op == 'project':
        project(pid, codegen_config, sm_peer, filesize)



