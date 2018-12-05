import yaml
import json
import argparse
import os
from typing import Callable, Dict

from conclave import generate_and_dispatch
from conclave import CodeGenConfig
from conclave.config import SparkConfig
from conclave.config import OblivcConfig
from conclave.config import NetworkConfig
from conclave.config import SwiftConfig
from conclave.config import JiffConfig
from conclave.swift import SwiftData


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
        # dv conf
        pass

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


def download_swift_data(conclave_config):
    """
    Download data from Swift to local filesystem.
    """

    swift_cfg = conclave_config.system_configs['swift'].source
    data_dir = conclave_config.input_path
    container = swift_cfg['data']['container_name']
    files = swift_cfg['data']['files']

    swift_data = SwiftData(swift_cfg)

    if files is not None:
        for file in files:
            swift_data.get_data(container, file, data_dir)

    swift_data.close_connection()


def post_swift_data(conclave_config):
    """
    Store locally held data on Swift.

    NOTE: if container_name doesn't exist, raises swiftclient.exceptions.ClientException

    Should check to see if container exists in the future, and create it if it doesn't exist.
    """
    input_swift_data = conclave_config.system_configs['swift'].source['data']['files']

    swift_cfg = conclave_config.system_configs['swift'].dest
    data_dir = conclave_config.input_path
    container = swift_cfg['data']['container_name']

    swift_data = SwiftData(swift_cfg)

    # this pushes all intermediate files to swift as well, will need some
    # way to identify only final output files in the future
    for subdir, dirs, files in os.walk(data_dir):
        for file in files:
            print(file)
            if file[0] != '.':
                if file not in input_swift_data:
                    swift_data.put_data(container, file, data_dir)

    swift_data.close_connection()


def run(protocol: Callable, mpc_framework: str="jiff", local_framework: str="python"):
    """
    Load parameters from config, download data from Swift,
    dispatch computation, and push results back to Swift.
    """

    parser = argparse.ArgumentParser(description="Run new workflow for Conclave.")
    parser.add_argument("--conf", metavar="/config/file.yml", type=str,
                        help="path of the config file", default="conf-ca.json", required=False)

    args = parser.parse_args()

    with open(args.conf) as fp:
        conf = json.load(fp)

    conclave_config = setup(conf)

    if conclave_config.data_backend == "swift":
        download_swift_data(conclave_config)
        generate_and_dispatch(protocol, conclave_config, [mpc_framework], [local_framework], apply_optimizations=False)
        post_swift_data(conclave_config)

    elif conclave_config.data_backend == "dataverse":
        # dv download // post protocols
        pass

    else:

        generate_and_dispatch(protocol, conclave_config, [mpc_framework], [local_framework], apply_optimizations=False)


