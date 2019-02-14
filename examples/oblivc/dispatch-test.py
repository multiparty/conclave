import conclave.lang as sal
import sys

from conclave import generate_code, dispatch_jobs
from conclave.config import CodeGenConfig, OblivcConfig, NetworkConfig
from conclave.utils import defCol


def protocol():
    """
    Define inputs and operations to be performed between them.
    """

    """
    Define input datasets
    """
    cols_in_a = [
        defCol('a', 'INTEGER', [1]),
        defCol('b', 'INTEGER', [1]),
    ]
    cols_in_b = [
        defCol('a', 'INTEGER', [2]),
        defCol('b', 'INTEGER', [2]),
    ]

    """
    Create input relations.
    """
    in1 = sal.create("in1", cols_in_a, {1})
    in2 = sal.create("in2", cols_in_b, {2})

    cc1 = sal.concat([in1, in2], 'cc1', ['a', 'b'])

    agg1 = sal.aggregate(cc1, "agg1", ['a'], "b", "sum", "b")

    sal.collect(agg1, 1)

    return {in1, in2}


if __name__ == "__main__":
    pid = sys.argv[1]

    # define name for the workflow
    workflow_name = "test-workflow-{}/".format(str(pid))
    # configure conclave
    conclave_config = CodeGenConfig(workflow_name, int(pid))

    net_conf = [
        {"host": "localhost", "port": 8000},
        {"host": "localhost", "port": 8001}
    ]
    net = NetworkConfig(net_conf, int(pid))
    conclave_config.with_network_config(net)
    conclave_config.use_leaky_ops = True

    oc_conf = OblivcConfig("/Users/ben/Desktop/dev/obliv-c/bin/oblivcc", "localhost:9000")
    conclave_config.with_oc_config(oc_conf)

    conclave_config.code_path = "/tmp/{}-one".format(workflow_name)
    conclave_config.input_path = "/Users/ben/Desktop/oc_input/"
    conclave_config.output_path = "/Users/ben/Desktop/oc_input/"

    job_queue = generate_code(protocol, conclave_config, ["obliv-c"], ["python"], apply_optimizations=True)
    dispatch_jobs(job_queue, conclave_config)
