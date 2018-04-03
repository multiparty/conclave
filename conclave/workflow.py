import yaml
import argparse
from typing import Callable, Dict

from conclave import generate_and_dispatch
from conclave import CodeGenConfig
from conclave.config import SharemindCodeGenConfig
from conclave.config import SparkConfig
from conclave.config import OblivcConfig
from conclave.config import NetworkConfig
from conclave.config import SwiftConfig
from conclave.swift import GetData, PutData


def setup(conf: Dict):

    # GENERAL
    pid = conf["pid"]
    workflow_name = conf["workflow_name"]

    # SWIFT
    cfg = conf["swift"]
    swift_config = SwiftConfig(cfg)

    # SPARK
    hdfs_path = conf["spark"]["hdfs"]["path"]
    spark_master_url = conf["spark"]["master_url"]
    spark_config = SparkConfig(spark_master_url)

    # SHAREMIND
    sm_config = SharemindCodeGenConfig(conf["code_path"])

    # OBLIV-C
    oc_path = conf["oblivc"]["oc_path"]
    ip_port = conf["oblivc"]["ip_port"]
    oc_config = OblivcConfig(oc_path, ip_port)

    # NET
    hosts = conf["sharemind"]["parties"]
    net_config = NetworkConfig(hosts, pid)

    # CONCLAVE SYSTEM CONFIG
    conclave_config = CodeGenConfig(workflow_name) \
        .with_sharemind_config(sm_config) \
        .with_spark_config(spark_config) \
        .with_oc_config(oc_config) \
        .with_swift_config(swift_config) \
        .with_network_config(net_config)

    conclave_config.code_path = conf["code_path"] + workflow_name
    conclave_config.input_path = conf["data_path"] + conf["name"]
    conclave_config.output_path = conf["data_path"] + conf["name"]
    conclave_config.pid = pid
    conclave_config.name = workflow_name

    return conclave_config


def download_data(conclave_config):
    """
    Download data from Swift to local filesystem.
    """

    swift_cfg = conclave_config['swift']['source']

    obj = GetData(swift_cfg, conclave_config.input_path)
    obj.get_data()


def post_data(conclave_config):
    """
    Store data held locally on Swift.
    """

    swift_cfg = conclave_config['swift']['destination']

    obj = PutData(swift_cfg, conclave_config.output_path)
    obj.store_data()


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

    download_data(conclave_config)

    generate_and_dispatch(protocol, conclave_config, ["sharemind"], ["spark"])

    post_data(conclave_config)


# Custom yaml join
def join(loader, node):
    seq = loader.construct_sequence(node)
    return "".join([str(i) for i in seq])

