from conclave import CodeGenConfig
from conclave import generate_and_dispatch
from conclave.config import JiffConfig
from conclave.config import NetworkConfig
from conclave.config import OblivcConfig
from conclave.config import SparkConfig


def setup(conf: dict):
    # GENERAL
    pid = int(conf["user_config"]["pid"])
    workflow_name = conf["user_config"]["workflow_name"]
    all_pids = conf["user_config"]['all_pids']
    use_leaky = conf["user_config"]["leaky_ops"]
    use_floats = conf["user_config"]["use_floats"]

    conclave_config = CodeGenConfig(workflow_name)

    # SPARK
    try:
        spark_avail = conf["backends"]["spark"]["available"]
        if spark_avail:
            spark_master_url = conf["backends"]["spark"]["master_url"]
            spark_config = SparkConfig(spark_master_url)
            conclave_config.with_spark_config(spark_config)
    except KeyError:
        pass

    # OBLIV-C
    try:
        oc_avail = conf["backends"]["oblivc"]["available"]
        if oc_avail:
            oc_path = conf["backends"]["oblivc"]["oc_path"]
            ip_port = conf["backends"]["oblivc"]["ip_port"]
            oc_config = OblivcConfig(oc_path, ip_port)
            conclave_config.with_oc_config(oc_config)
    except KeyError:
        pass

    # JIFF
    try:
        jiff_avail = conf["backends"]["jiff"]["available"]
        if jiff_avail:
            jiff_path = conf["backends"]["jiff"]["jiff_path"]
            party_count = len(all_pids)
            server_ip = conf["backends"]["jiff"]["server_ip"]
            server_pid = conf["backends"]["jiff"]["server_pid"]
            server_port = conf["backends"]["jiff"]["server_port"]
            jiff_config = JiffConfig(jiff_path, party_count, server_ip, server_port, server_pid)
            conclave_config.with_jiff_config(jiff_config)
    except KeyError:
        pass

    # NET
    hosts = conf["net"]["parties"]
    net_config = NetworkConfig(hosts, pid)
    conclave_config.with_network_config(net_config)

    conclave_config.pid = pid
    conclave_config.all_pids = all_pids
    conclave_config.name = workflow_name
    conclave_config.use_leaky_ops = use_leaky
    conclave_config.use_floats = use_floats

    conclave_config.input_path = conf["user_config"]["paths"]["input_path"]
    conclave_config.code_path = conf["user_config"]["paths"]["input_path"]
    conclave_config.output_path = conf["user_config"]["paths"]["input_path"]

    return conclave_config


def run(protocol: callable, conf, mpc_framework: str = "obliv-c",
        local_framework: str = "python", apply_optimisations=False):
    """
    Load parameters from config & dispatch computation.
    Downloads files if necessary from either Dataverse or Swift
    """

    conclave_config = setup(conf)

    generate_and_dispatch(
        protocol, conclave_config, [mpc_framework], [local_framework], apply_optimizations=apply_optimisations
    )
