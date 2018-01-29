import yaml
import argparse
from conclave import generate_and_dispatch
from conclave import CodeGenConfig
from conclave.config import SharemindCodeGenConfig
from conclave.config import SparkConfig
from conclave.config import NetworkConfig
from typing import Callable, Dict


def setup(conf: Dict):
    pid = conf["pid"]
    hdfs_node_name = conf["spark"]["hdfs"]["node_name"]
    hdfs_root = conf["spark"]["hdfs"]["root"]
    spark_master_url = conf["spark"]["master_url"]

    workflow_name = conf["workflow_name"]

    sm_config = SharemindCodeGenConfig(conf["code_path"])
    spark_config = SparkConfig(spark_master_url)

    conclave_config = CodeGenConfig(workflow_name) \
        .with_sharemind_config(sm_config) \
        .with_spark_config(spark_config)

    conclave_config.code_path = conf["code_path"] + workflow_name
    conclave_config.input_path = "hdfs://{}/{}/{}".format(
        hdfs_node_name, hdfs_root, conf["name"])
    conclave_config.output_path = "hdfs://{}/{}/{}".format(
        hdfs_node_name, hdfs_root, conf["name"])
    conclave_config.pid = pid
    conclave_config.name = workflow_name

    network_config = NetworkConfig(conf["sharemind"]["parties"], pid)

    conclave_config.with_network_config(network_config)

    return conclave_config


def run(protocol: Callable):
    # Parse arguments
    parser = argparse.ArgumentParser(description="Run new workflow for Conclave.")
    parser.add_argument("--conf", metavar="/config/file.yml", type=str,
                        help="path of the config file", default="conf-ca.yml", required=False)

    args = parser.parse_args()

    # Load config file
    with open(args.conf) as fp:
        yaml.add_constructor("!join", join)
        conf = yaml.load(fp)

    # Setup conclave
    conclave_config = setup(conf)

    generate_and_dispatch(protocol, conclave_config, ["sharemind"], ["spark"])


# Custom yaml join
def join(loader, node):
    seq = loader.construct_sequence(node)
    return "".join([str(i) for i in seq])

