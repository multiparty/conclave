import os
import sys

import conclave.lang as cc
from conclave import generate_code, dispatch_jobs
from conclave.config import CodeGenConfig, SharemindCodeGenConfig
from conclave.utils import defCol


def protocol():
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

    right_medication_cols = [defCol(str(i), "INTEGER", [2]) for i in range(num_med_cols)]
    right_medication = cc.create("right_medication", right_medication_cols, {2})
    right_diagnosis_cols = [defCol(str(i + num_med_cols), "INTEGER", [2]) for i in range(num_diag_cols)]
    right_diagnosis = cc.create("right_diagnosis", right_diagnosis_cols, {2})

    # only keep relevant columns
    left_medication_proj = cc.project(left_medication, "left_medication_proj",
                                      [pid_col_meds, med_col_meds, date_col_meds])
    left_diagnosis_proj = cc.project(left_diagnosis, "left_diagnosis_proj",
                                     [pid_col_diags, diag_col_diags, date_col_diags])

    right_medication_proj = cc.project(right_medication, "right_medication_proj",
                                       [pid_col_meds, med_col_meds, date_col_meds])
    right_diagnosis_proj = cc.project(right_diagnosis, "right_diagnosis_proj",
                                      [pid_col_diags, diag_col_diags, date_col_diags])

    left_join = cc._pub_join(left_medication_proj, "left_join", pid_col_meds,
                             other_op_node=left_diagnosis_proj)
    right_join = cc._pub_join(right_medication_proj, "right_join", pid_col_meds, is_server=False,
                              other_op_node=right_diagnosis_proj)
    joined = cc.concat_cols([left_join, right_join], "joined", use_mult=True)

    # do filters after the join
    cases = cc.cc_filter(joined, "cases", date_col_diags, "<", other_col_name=date_col_meds)
    aspirin = cc.cc_filter(cases, "aspirin", med_col_meds, "==", scalar=1)
    heart_patients = cc.cc_filter(aspirin, "heart_patients", diag_col_diags, "==", scalar=1)

    cc.collect(cc.distinct_count(heart_patients, "actual", pid_col_meds, use_sort=False), 1)

    return {
        left_medication,
        left_diagnosis,
        right_medication,
        right_diagnosis
    }


def run_local():
    def local_protocol():
        pid_col_meds = "0"
        med_col_meds = "4"
        date_col_meds = "7"

        pid_col_diags = "8"
        diag_col_diags = "16"
        date_col_diags = "18"

        num_med_cols = 8
        medication_cols = [defCol(str(i), "INTEGER", [1]) for i in range(num_med_cols)]

        medication = cc.create("left_medication", medication_cols, {1})

        num_diag_cols = 13
        diagnosis_cols = [defCol(str(i + num_med_cols), "INTEGER", [1]) for i in range(num_diag_cols)]

        diagnosis = cc.create("left_diagnosis", diagnosis_cols, {1})

        # only keep relevant columns
        medication_proj = cc.project(medication, "medication_proj", [pid_col_meds, med_col_meds, date_col_meds])
        diagnosis_proj = cc.project(diagnosis, "diagnosis_proj", [pid_col_diags, diag_col_diags, date_col_diags])

        aspirin = cc.cc_filter(medication_proj, "aspirin", med_col_meds, "==", scalar=1)
        heart_patients = cc.cc_filter(diagnosis_proj, "heart_patients", diag_col_diags, "==", scalar=1)

        joined = cc.join(aspirin, heart_patients, "joined", [pid_col_meds], [pid_col_diags])
        cases = cc.cc_filter(joined, "cases", date_col_diags, "<", other_col_name=date_col_meds)

        cc.distinct_count(cases, "expected", pid_col_meds)

        return {medication, diagnosis}

    # define name for the workflow
    workflow_name = "aspirin-real-test-" + pid
    # configure conclave
    conclave_config = CodeGenConfig(workflow_name, int(pid))
    conclave_config.all_pids = [1]
    sharemind_conf = SharemindCodeGenConfig("/mnt/shared", use_docker=False, use_hdfs=False)
    conclave_config.with_sharemind_config(sharemind_conf)
    current_dir = os.path.dirname(os.path.realpath(__file__))
    # point conclave to the directory where the generated code should be stored/ read from
    conclave_config.code_path = os.path.join("/mnt/shared", workflow_name)
    # point conclave to directory where data is to be read from...
    conclave_config.input_path = os.path.join(current_dir, data_root)
    # and written to
    conclave_config.output_path = os.path.join(current_dir, data_root)
    # define this party's unique ID (in this demo there is only one party)
    job_queue = generate_code(local_protocol, conclave_config, ["sharemind"], ["python"], apply_optimizations=False)
    dispatch_jobs(job_queue, conclave_config)


if __name__ == "__main__":
    pid = sys.argv[1]
    data_root = sys.argv[2]
    print(data_root)
    # define name for the workflow
    workflow_name = "aspirin-large-join-" + pid + "-" + data_root
    # configure conclave
    conclave_config = CodeGenConfig(workflow_name, int(pid))
    conclave_config.network_config = {
        "pid": int(pid),
        "parties": {
            1: {"host": "ca-spark-node-0", "port": 9001},
            2: {"host": "cb-spark-node-0", "port": 9002},
            3: {"host": "cc-spark-node-0", "port": 9003}
        }
    }
    conclave_config.use_leaky_ops = False
    sharemind_conf = SharemindCodeGenConfig("/mnt/shared", use_docker=True, use_hdfs=False)
    conclave_config.with_sharemind_config(sharemind_conf)
    conclave_config.code_path = os.path.join("/mnt/shared", workflow_name)
    conclave_config.input_path = os.path.join("/mnt/shared", data_root)
    conclave_config.output_path = os.path.join("/mnt/shared", data_root)

    job_queue = generate_code(protocol, conclave_config, ["sharemind"], ["python"], apply_optimizations=True)
    dispatch_jobs(job_queue, conclave_config)

    if int(pid) == 1:
        run_local()
