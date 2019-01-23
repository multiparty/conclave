import json
import argparse
from typing import Callable, Dict

from conclave import generate_and_dispatch
from conclave import CodeGenConfig
from conclave.config import SparkConfig
from conclave.config import OblivcConfig
from conclave.config import NetworkConfig
from conclave.config import SwiftConfig
from conclave.config import DataverseConfig
from conclave.config import JiffConfig
from conclave.utils import *


def setup(conf: Dict):

    # GENERAL
    pid = conf["user_config"]["pid"]
    workflow_name = conf["user_config"]["workflow_name"]
    data_backend = conf["data"]["data_backend"]
    all_pids = conf["user_config"]['all_pids']

    # SPARK
    spark_master_url = conf["backends"]["spark"]["master_url"]
    spark_config = SparkConfig(spark_master_url)

    # OBLIV-C
    oc_path = conf["backends"]["oblivc"]["oc_path"]
    ip_port = conf["backends"]["oblivc"]["ip_port"]
    oc_config = OblivcConfig(oc_path, ip_port)

    # JIFF
    jiff_path = conf["backends"]["jiff"]["jiff_path"]
    party_count = conf["backends"]["jiff"]["party_count"]
    server_ip = conf["backends"]["jiff"]["server_ip"]
    server_port = conf["backends"]["jiff"]["server_port"]
    jiff_config = JiffConfig(jiff_path, party_count, server_ip, server_port)

    # NET
    hosts = conf["net"]["parties"]
    net_config = NetworkConfig(hosts, pid)

    # CONCLAVE SYSTEM CONFIG
    conclave_config = CodeGenConfig(workflow_name) \
        .with_spark_config(spark_config) \
        .with_oc_config(oc_config) \
        .with_jiff_config(jiff_config) \
        .with_network_config(net_config)

    if data_backend == "swift":
        cfg = conf["swift"]
        swift_config = SwiftConfig(cfg)
        conclave_config.with_swift_config(swift_config)

    elif data_backend == "dataverse":
        cfg = conf["dataverse"]
        dv_conf = DataverseConfig(cfg)
        conclave_config.with_dataverse_config(dv_conf)
        # dataverse converts all csv uploads to tsv
        conclave_config.delimiter = '\t'

    else:
        print("No remote data backend source listed. Using local storage.\n")

    conclave_config.pid = pid
    conclave_config.all_pids = all_pids
    conclave_config.name = workflow_name
    conclave_config.data_backend = data_backend

    conclave_config.code_path = conf["code_path"]
    conclave_config.output_path = conf["output_path"]
    conclave_config.input_path = conf["input_path"]

    return conclave_config


def run(protocol: Callable, mpc_framework: str = "jiff", local_framework: str = "python", apply_optimisations=False):
    """
    Load parameters from config & dispatch computation.
    Downloads files if necessary from either Dataverse or Swift
    """

    parser = argparse.ArgumentParser(description="Run new workflow for Conclave.")
    parser.add_argument("--conf", metavar="/config/file.json", type=str,
                        help="path of the config file", default="conf.json", required=False)

    args = parser.parse_args()

    with open(args.conf) as fp:
        conf = json.load(fp)

    conclave_config = setup(conf)

    if conclave_config.data_backend == "swift":
        download_swift_data(conclave_config)
        generate_and_dispatch(
            protocol, conclave_config, [mpc_framework], [local_framework], apply_optimizations=apply_optimisations
        )
        post_swift_data(conclave_config)

    elif conclave_config.data_backend == "dataverse":
        download_dataverse_data(conclave_config)
        generate_and_dispatch(
            protocol, conclave_config, [mpc_framework], [local_framework], apply_optimizations=apply_optimisations
        )
        post_dataverse_data(conclave_config)

    else:
        generate_and_dispatch(
            protocol, conclave_config, [mpc_framework], [local_framework], apply_optimizations=apply_optimisations
        )
