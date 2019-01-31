import os
import sys

import conclave.lang as cc
from conclave import generate_code, dispatch_jobs
from conclave.config import CodeGenConfig, OblivcConfig, NetworkConfig
from conclave.utils import defCol


def protocol_mpc():
    pid_col_meds = "0"
    med_col_meds = "4"
    date_col_meds = "7"

    pid_col_diags = "8"
    diag_col_diags = "16"
    date_col_diags = "18"

    num_med_cols = 8
    num_diag_cols = 13

    left_medication_cols = [defCol(str(i), "INTEGER", [1]) for i in range(num_med_cols)]
    left_medication = cc.create("left_medication", left_medication_cols, {1})
    left_diagnosis_cols = [defCol(str(i + num_med_cols), "INTEGER", [1]) for i in range(num_diag_cols)]
    left_diagnosis = cc.create("left_diagnosis", left_diagnosis_cols, {1})
    left_keys = cc.union(left_medication, left_diagnosis, "left_pids", pid_col_meds, pid_col_diags)

    right_medication_cols = [defCol(str(i), "INTEGER", [2]) for i in range(num_med_cols)]
    right_medication = cc.create("right_medication", right_medication_cols, {2})
    right_diagnosis_cols = [defCol(str(i + num_med_cols), "INTEGER", [2]) for i in range(num_diag_cols)]
    right_diagnosis = cc.create("right_diagnosis", right_diagnosis_cols, {2})
    right_keys = cc.union(right_medication, right_diagnosis, "right_pids", pid_col_meds, pid_col_diags)

    left_shared_pids = cc._pub_intersect(
        left_keys, "a_left_shared_pids", pid_col_meds, host='10.10.10.17')
    cc._persist(left_shared_pids, "a_left_shared_pids")
    right_shared_pids = cc._pub_intersect(
        right_keys, "a_right_shared_pids", pid_col_meds, is_server=False, host='10.10.10.17')
    cc._persist(right_shared_pids, "a_right_shared_pids")

    left_medication_proj = cc.project(left_medication, "left_medication_proj",
                                      [pid_col_meds, med_col_meds, date_col_meds])
    left_medication_shared = cc.filter_by(left_medication_proj, "left_medication_shared", pid_col_meds,
                                          left_shared_pids)

    left_diagnosis_proj = cc.project(left_diagnosis, "left_diagnosis_proj",
                                     [pid_col_diags, diag_col_diags, date_col_diags])
    left_diagnosis_shared = cc.filter_by(left_diagnosis_proj, "left_diagnosis_shared", pid_col_diags, left_shared_pids)

    right_medication_proj = cc.project(right_medication, "right_medication_proj",
                                       [pid_col_meds, med_col_meds, date_col_meds])
    right_medication_shared = cc.filter_by(right_medication_proj, "right_medication_shared", pid_col_meds,
                                           right_shared_pids)

    right_diagnosis_proj = cc.project(right_diagnosis, "right_diagnosis_proj",
                                      [pid_col_diags, diag_col_diags, date_col_diags])
    right_diagnosis_shared = cc.filter_by(right_diagnosis_proj, "right_diagnosis_shared", pid_col_diags,
                                          right_shared_pids)

    left_join = cc._pub_join(left_medication_shared, "left_join", pid_col_meds,
                             other_op_node=left_diagnosis_shared, host='10.10.10.17')
    right_join = cc._pub_join(right_medication_shared, "right_join", pid_col_meds, is_server=False,
                              other_op_node=right_diagnosis_shared, host='10.10.10.17')
    joined = cc.concat_cols([left_join, right_join], "joined", use_mult=True)

    # do filters after the join
    cases = cc.cc_filter(joined, "cases", date_col_diags, "<", other_col_name=date_col_meds)
    aspirin = cc.cc_filter(cases, "aspirin", med_col_meds, "==", scalar=1)
    heart_patients = cc.cc_filter(aspirin, "heart_patients", diag_col_diags, "==", scalar=1)

    cc.collect(cc.distinct_count(heart_patients, "actual_mpc", pid_col_meds, use_sort=False), 1)

    return {
        left_medication,
        left_diagnosis,
        right_medication,
        right_diagnosis
    }


def protocol_local(suffix: str, pid: int):
    pid_col_meds = "0"
    med_col_meds = "4"
    date_col_meds = "7"

    pid_col_diags = "8"
    diag_col_diags = "16"
    date_col_diags = "18"

    num_med_cols = 8
    num_diag_cols = 13

    left_medication_cols = [defCol(str(i), "INTEGER", pid) for i in range(num_med_cols)]
    medication = cc.create(suffix + "_medication", left_medication_cols, {pid})
    left_diagnosis_cols = [defCol(str(i + num_med_cols), "INTEGER", pid) for i in range(num_diag_cols)]
    diagnosis = cc.create(suffix + "_diagnosis", left_diagnosis_cols, {pid})

    shared_pids = cc.create("a_{}_shared_pids".format(suffix), [defCol(pid_col_meds, "INTEGER", pid)], {pid})

    # only keep relevant columns
    medication_proj = cc.project(medication, "medication_proj", [pid_col_meds, med_col_meds, date_col_meds])
    medication_mine = cc.filter_by(medication_proj, "medication_mine", pid_col_meds, shared_pids, use_not_in=True)

    diagnosis_proj = cc.project(diagnosis, "diagnosis_proj", [pid_col_diags, diag_col_diags, date_col_diags])
    diagnosis_mine = cc.filter_by(diagnosis_proj, "diagnosis_mine", pid_col_diags, shared_pids, use_not_in=True)

    joined = cc.join(medication_mine, diagnosis_mine, "joined", [pid_col_meds], [pid_col_diags])

    cases = cc.cc_filter(joined, "cases", date_col_diags, "<", other_col_name=date_col_meds)
    aspirin = cc.cc_filter(cases, "aspirin", med_col_meds, "==", scalar=1)
    heart_patients = cc.cc_filter(aspirin, "heart_patients", diag_col_diags, "==", scalar=1)

    cc.distinct_count(heart_patients, "actual_" + suffix, pid_col_meds)

    return {medication, diagnosis}


def run_mpc(pid: str, data_root: str):
    workflow_name = "aspirin-mpc-join-" + pid + "-" + data_root
    conclave_config = CodeGenConfig(workflow_name, int(pid))
    conclave_config.use_leaky_ops = False
    conclave_config.all_pids = [1, 2]

    oc_conf = OblivcConfig("/home/ubuntu/obliv-c/bin/oblivcc", "10.10.10.17:9000")
    conclave_config.with_oc_config(oc_conf)

    net_conf = [
        {"host": "10.10.10.17", "port": 8000},
        {"host": "10.10.10.11", "port": 8000}
    ]
    net = NetworkConfig(net_conf, int(pid))
    conclave_config.with_network_config(net)

    conclave_config.code_path = os.path.join("/mnt/shared", workflow_name)
    conclave_config.input_path = os.path.join("/mnt/shared", data_root)
    conclave_config.output_path = os.path.join("/mnt/shared", data_root)

    job_queue = generate_code(protocol_mpc, conclave_config, ["obliv-c"], ["python"], apply_optimizations=True)
    dispatch_jobs(job_queue, conclave_config)


def run_local(pid: str, data_root: str):
    workflow_name = "aspirin-local-join-" + pid + "-" + data_root
    conclave_config = CodeGenConfig(workflow_name, int(pid))
    conclave_config.all_pids = [int(pid)]

    oc_conf = OblivcConfig("/home/ubuntu/obliv-c/bin/oblivcc", "10.10.10.17:9000")
    conclave_config.with_oc_config(oc_conf)

    conclave_config.code_path = os.path.join("/mnt/shared", workflow_name)
    conclave_config.input_path = os.path.join("/mnt/shared", data_root)
    conclave_config.output_path = os.path.join("/mnt/shared", data_root)

    suffix = "left" if pid == "1" else "right"

    job_queue = generate_code(lambda: protocol_local(suffix, int(pid)), conclave_config, ["obliv-c"], ["python"],
                              apply_optimizations=False)
    dispatch_jobs(job_queue, conclave_config)


if __name__ == "__main__":
    top_pid = sys.argv[1]
    run_mpc(top_pid, sys.argv[2])
    if top_pid in {"1", "2"}:
        run_local(top_pid, sys.argv[2])
