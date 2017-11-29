import yaml
import argparse
from salmon import codegen
import salmon.dispatch
import salmon.net
from salmon.codegen import CodeGenConfig
from salmon.codegen.sharemind import SharemindCodeGenConfig


def setup(conf):
    pid = conf["pid"]
    hdfs_node_name = conf["spark"]["hdfs"]["node_name"]
    hdfs_root = conf["spark"]["hdfs"]["root"]
    spark_master_url = conf["spark"]["master_url"]

    workflow_name = conf["workflow_name"]
    config = {
        "name": conf["name"],
        "pid": pid,
        "delimiter": conf["delimiter"],
        "code_path": conf["code_path"] + workflow_name,
        "input_path": "hdfs://{}/{}/{}".format(hdfs_node_name, hdfs_root, conf["name"]),
        "output_path": "hdfs://{}/{}/{}".format(hdfs_node_name, hdfs_root, conf["name"]),
    }
    sm_cg_config = SharemindCodeGenConfig(workflow_name, conf["code_path"])
    codegen_config = CodeGenConfig(
        workflow_name).with_sharemind_config(sm_cg_config)
    codegen_config.code_path = conf["code_path"] + workflow_name
    codegen_config.input_path = "hdfs://{}/{}/{}".format(
        hdfs_node_name, hdfs_root, conf["name"])
    codegen_config.output_path = "hdfs://{}/{}/{}".format(
        hdfs_node_name, hdfs_root, conf["name"])

    codegen_config.pid = pid
    codegen_config.name = workflow_name

    sharemind_config = {
        "pid": pid,
        "parties": conf["sharemind"]["parties"]
    }
    sharemind_peer = salmon.net.setup_peer(sharemind_config)

    return config, sharemind_peer, spark_master_url


def run(protocol):
    # Parse arguments
    parser = argparse.ArgumentParser(description="Run new workflow for Conclave.")
    parser.add_argument("--conf", metavar="/config/file.yml", type=str,
                        help="path of the config file", default="conf.yml", required=False)

    args = parser.parse_args()

    # Load config file
    with open(args.conf) as fp:
        yaml.add_constructor("!join", join)
        conf = yaml.load(fp)

    # Setup conclave
    config, sharemind_peer, spark_master_url = setup(conf)

    jobqueue = codegen(protocol, config, ["sharemind"], ["spark"])
    print(jobqueue)

    salmon.dispatch.dispatch_all(spark_master_url, sharemind_peer, jobqueue)


# Custom yaml join
def join(loader, node):
    seq = loader.construct_sequence(node)
    return "".join([str(i) for i in seq])


if __name__ == "__main__":
    run(None)
