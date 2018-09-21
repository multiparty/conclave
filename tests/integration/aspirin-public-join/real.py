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
    medication_cols = [defCol(str(i), "INTEGER", [1]) for i in range(num_med_cols)]

    medication = cc.create("medication", medication_cols, {1})

    num_diag_cols = 13
    diagnosis_cols = [defCol(str(i + num_med_cols), "INTEGER", [2]) for i in range(num_diag_cols)]

    diagnosis = cc.create("diagnosis", diagnosis_cols, {2})

    # only keep relevant columns
    medication_proj = cc.project(medication, "medication_proj", [pid_col_meds, med_col_meds, date_col_meds])
    diagnosis_proj = cc.project(diagnosis, "diagnosis_proj", [pid_col_diags, diag_col_diags, date_col_diags])

    left_join = cc._pub_join(medication_proj, "left_medication", pid_col_meds)
    right_join_proj = cc.project(
        cc._pub_join(diagnosis_proj, "right_diagnosis", pid_col_diags, is_server=False), "right_diagnosis_proj",
        [diag_col_diags, date_col_diags]
    )
    joined = cc.concat_cols([left_join, right_join_proj], "joined")

    # do filters after the join
    cases = cc.cc_filter(joined, "cases", date_col_diags, "<", other_col_name=date_col_meds)
    aspirin = cc.cc_filter(cases, "aspirin", med_col_meds, "==", scalar=1)
    heart_patients = cc.cc_filter(aspirin, "heart_patients", diag_col_diags, "==", scalar=1)

    cc.collect(cc.distinct_count(heart_patients, "actual", pid_col_meds), 1)

    return {medication, diagnosis}


if __name__ == "__main__":
    pid = sys.argv[1]
    try:
        use_leaky = sys.argv[2] == "-l"
    except Exception:
        use_leaky = False
    # define name for the workflow
    workflow_name = "real-aspirin-test-" + pid
    # configure conclave
    conclave_config = CodeGenConfig(workflow_name, int(pid))
    conclave_config.all_pids = [1, 2, 3]
    conclave_config.use_leaky_ops = use_leaky
    sharemind_conf = SharemindCodeGenConfig("/mnt/shared",
                                            use_docker=True,
                                            use_hdfs=False)
    conclave_config.with_sharemind_config(sharemind_conf)
    current_dir = os.path.dirname(os.path.realpath(__file__))
    # point conclave to the directory where the generated code should be stored/ read from
    conclave_config.code_path = os.path.join("/mnt/shared", workflow_name)
    # point conclave to directory where data is to be read from...
    conclave_config.input_path = os.path.join(current_dir, "data")
    # and written to
    conclave_config.output_path = os.path.join(current_dir, "data")
    job_queue = generate_code(protocol, conclave_config, ["sharemind"], ["python"], apply_optimizations=True)
    dispatch_jobs(job_queue, conclave_config)
