import os
import sys

import conclave.lang as cc
from conclave import CodeGenConfig, generate_and_dispatch
from conclave.config import SharemindCodeGenConfig
from conclave.utils import defCol


def protocol():
    cols_in_1 = [
        defCol("companyID", "INTEGER", [1]),
        defCol("price", "INTEGER", [1])
    ]
    in1 = cc.create("in1", cols_in_1, {1})
    cols_in_2 = [
        defCol("companyID", "INTEGER", [2]),
        defCol("price", "INTEGER", [2])
    ]
    in2 = cc.create("in2", cols_in_2, {2})
    cols_in_3 = [
        defCol("companyID", "INTEGER", [3]),
        defCol("price", "INTEGER", [3])
    ]
    in3 = cc.create("in3", cols_in_3, {3})

    cab_data = cc.concat([in1, in2, in3], "cab_data")

    selected_input = cc.project(cab_data, "selected_input", ["companyID", "price"])
    local_rev = cc.aggregate(selected_input, "local_rev", ["companyID"], "price", "sum", "local_rev")
    scaled_down = cc.divide(local_rev, "scaled_down", "local_rev", ["local_rev", 1000])
    first_val_blank = cc.multiply(scaled_down, "first_val_blank", "companyID", ["companyID", 0])
    local_rev_scaled = cc.multiply(first_val_blank, "local_rev_scaled", "local_rev", ["local_rev", 100])
    total_rev = cc.aggregate(first_val_blank, "total_rev", ["companyID"], "local_rev", "sum", "global_rev")
    local_total_rev = cc.join(local_rev_scaled, total_rev, "local_total_rev", ["companyID"], ["companyID"])
    market_share = cc.divide(local_total_rev, "market_share", "local_rev", ["local_rev", "global_rev"])
    market_share_squared = cc.multiply(market_share, "market_share_squared", "local_rev", ["local_rev", "local_rev", 1])
    hhi = cc.aggregate(market_share_squared, "hhi", ["companyID"], "local_rev", "sum", "hhi")

    cc.collect(hhi, 1)

    # return root nodes
    return {in1, in2, in3}


if __name__ == "__main__":
    pid = sys.argv[1]
    data_root = sys.argv[2]
    workflow_name = "hhi-benchmark" + pid + "-" + data_root
    conclave_config = CodeGenConfig(workflow_name, int(pid))
    sharemind_conf = SharemindCodeGenConfig("/mnt/shared", use_docker=True, use_hdfs=False)
    conclave_config.with_sharemind_config(sharemind_conf)
    current_dir = os.path.dirname(os.path.realpath(__file__))
    conclave_config.code_path = os.path.join("/mnt/shared", workflow_name)
    conclave_config.input_path = os.path.join("/mnt/shared", data_root)
    conclave_config.output_path = os.path.join("/mnt/shared", data_root)
    local_backend = "python"
    generate_and_dispatch(protocol, conclave_config, ["sharemind"], [local_backend], apply_optimizations=True)
