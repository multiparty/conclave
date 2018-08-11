import yaml
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
    pid = conf["pid"]
    workflow_name = conf["workflow_name"]
    use_swift = conf["use_swift"]
    all_pids = conf['all_pids']

    # SWIFT
    cfg = conf["swift"]
    swift_config = SwiftConfig(cfg)

    # SPARK
    spark_master_url = conf["spark"]["master_url"]
    spark_config = SparkConfig(spark_master_url)

    # OBLIV-C
    oc_path = conf["oblivc"]["oc_path"]
    ip_port = conf["oblivc"]["ip_port"]
    oc_config = OblivcConfig(oc_path, ip_port)

    # JIFF
    jiff_path = conf["jiff"]["jiff_path"]
    party_count = conf["jiff"]["party_count"]
    server_ip = conf["jiff"]["server_ip"]
    server_port = conf["jiff"]["server_port"]
    jiff_config = JiffConfig(jiff_path, party_count, server_ip, server_port)

    # NET
    hosts = conf["net"]["parties"]
    net_config = NetworkConfig(hosts, pid)

    # CONCLAVE SYSTEM CONFIG
    conclave_config = CodeGenConfig(workflow_name) \
        .with_spark_config(spark_config) \
        .with_oc_config(oc_config) \
        .with_swift_config(swift_config) \
        .with_jiff_config(jiff_config) \
        .with_network_config(net_config)

    conclave_config.pid = pid
    conclave_config.all_pids = all_pids
    conclave_config.name = workflow_name
    conclave_config.use_swift = use_swift

    conclave_config.code_path = conf["code_path"]
    conclave_config.output_path = conf["output_path"]
    conclave_config.input_path = conf["input_path"]

    return conclave_config


def download_data(conclave_config):
    """
    Download data from Swift to local filesystem.
    """

    swift_cfg = conclave_config.system_configs['swift'].source
    data_dir = conclave_config.input_path
    container = swift_cfg['DATA']['container_name']
    files = swift_cfg['DATA']['files']

    swift_data = SwiftData(swift_cfg)

    if files is not None:
        for file in files:
            swift_data.get_data(container, file, data_dir)

    swift_data.close_connection()


def post_data(conclave_config):
    """
    Store locally held data on Swift.

    NOTE: if container_name doesn't exist, raises swiftclient.exceptions.ClientException

    Should check to see if container exists in the future, and create it if it doesn't exist.
    """
    input_swift_data = conclave_config.system_configs['swift'].source['DATA']['files']

    swift_cfg = conclave_config.system_configs['swift'].dest
    data_dir = conclave_config.input_path
    container = swift_cfg['DATA']['container_name']

    swift_data = SwiftData(swift_cfg)

    # hack hack hack
    for subdir, dirs, files in os.walk(data_dir):
        for file in files:
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
                        help="path of the config file", default="conf-ca.yml", required=False)

    args = parser.parse_args()

    with open(args.conf) as fp:
        conf = yaml.load(fp)

    conclave_config = setup(conf)

    if conclave_config.use_swift:
        download_data(conclave_config)
        generate_and_dispatch(protocol, conclave_config, [mpc_framework], [local_framework])
        post_data(conclave_config)

    else:

        generate_and_dispatch(protocol, conclave_config, [mpc_framework], [local_framework], apply_optimizations=False)


