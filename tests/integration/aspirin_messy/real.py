import os
import sys

import conclave.lang as cc
from conclave import generate_code, dispatch_jobs
from conclave.config import CodeGenConfig, SharemindCodeGenConfig
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

    shared_pids = cc._pub_intersect(left_keys, "shared_pids", pid_col_meds)
    shared_pids_2 = cc._pub_intersect(right_keys, "shared_pids_2", pid_col_meds, is_server=False)

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

    cc.collect(cc.distinct_count(heart_patients, "actual_mpc", pid_col_meds, use_sort=False), 1)

    return {
        left_medication,
        left_diagnosis,
        right_medication,
        right_diagnosis
    }


def write_rel(job_dir, rel_name, rel, schema_header):
    print("Will write to {}/{}".format(job_dir, rel_name))
    path = "{}/{}".format(job_dir, rel_name)
    with open(path, "w") as f:
        # hack header
        f.write(schema_header + "\n")
        for row in rel:
            f.write(",".join([str(val) for val in row]) + "\n")


def read_rel(path_to_rel):
    rows = []
    with open(path_to_rel, "r") as f:
        it = iter(f.readlines())
        for raw_row in it:
            # TODO: only need to do this for first row
            try:
                split_row = [int(val) for val in raw_row.split(",")]
                rows.append([int(val) for val in split_row])
            except ValueError:
                print("skipped header")
    return rows


def project(rel, selected_cols):
    return [[row[idx] for idx in selected_cols] for row in rel]


def filter_by_not_keys(rel: list, keys: set, key_col: int):
    return [row for row in rel if row[key_col] not in keys]


def local(input_path: str, output_path: str, suffix: str):
    my_medications = read_rel(input_path + "/" + "{}_medication.csv".format(suffix))
    my_diagnosis = read_rel(input_path + "/" + "{}_diagnosis.csv".format(suffix))

    keys = read_rel(input_path + "/" + "shared_pids.csv".format(suffix))

    my_medications = filter_by_not_keys(my_medications, keys, 0)
    my_diagnosis = filter_by_not_keys(my_diagnosis, keys, 0)

    write_rel(output_path, "my_meds_{}.csv".format(suffix), my_medications, '')
    write_rel(output_path, "my_diags_{}.csv".format(suffix), my_diagnosis, '')


def protocol(suffix: str):
    pid_col_meds = "0"
    med_col_meds = "4"
    date_col_meds = "7"

    pid_col_diags = "8"
    diag_col_diags = "16"
    date_col_diags = "18"

    num_med_cols = 8
    num_diag_cols = 13

    left_medication_cols = [defCol(str(i), "INTEGER", [1]) for i in range(num_med_cols)]
    medication = cc.create("my_meds_" + suffix, left_medication_cols, {1})
    left_diagnosis_cols = [defCol(str(i + num_med_cols), "INTEGER", [1]) for i in range(num_diag_cols)]
    diagnosis = cc.create("my_diags_" + suffix, left_diagnosis_cols, {1})

    # only keep relevant columns
    medication_proj = cc.project(medication, "medication_proj", [pid_col_meds, med_col_meds, date_col_meds])
    diagnosis_proj = cc.project(diagnosis, "diagnosis_proj", [pid_col_diags, diag_col_diags, date_col_diags])

    joined = cc.join(medication_proj, diagnosis_proj, "joined", [pid_col_meds], [pid_col_diags])

    cases = cc.cc_filter(joined, "cases", date_col_diags, "<", other_col_name=date_col_meds)
    aspirin = cc.cc_filter(cases, "aspirin", med_col_meds, "==", scalar=1)
    heart_patients = cc.cc_filter(aspirin, "heart_patients", diag_col_diags, "==", scalar=1)

    cc.distinct_count(heart_patients, "actual_" + suffix, pid_col_meds)

    return {medication, diagnosis}


def local_main(pid: str):
    if pid == "1":
        # define name for the workflow
        workflow_name = "aspirin-simple-test-" + pid
        # configure conclave
        conclave_config = CodeGenConfig(workflow_name, int(pid))
        conclave_config.all_pids = [1]
        sharemind_conf = SharemindCodeGenConfig("/mnt/shared", use_docker=False, use_hdfs=False)
        conclave_config.with_sharemind_config(sharemind_conf)
        current_dir = os.path.dirname(os.path.realpath(__file__))
        # point conclave to the directory where the generated code should be stored/ read from
        conclave_config.code_path = os.path.join("/mnt/shared", workflow_name)
        # point conclave to directory where data is to be read from...
        conclave_config.input_path = os.path.join(current_dir, "data")
        # and written to
        conclave_config.output_path = os.path.join(current_dir, "data")
        for suffix in ["left", "right"]:
            # define this party's unique ID (in this demo there is only one party)
            job_queue = generate_code(lambda: protocol(suffix), conclave_config, ["sharemind"], ["python"],
                                      apply_optimizations=False)
            local(conclave_config.input_path, conclave_config.output_path, suffix)
            dispatch_jobs(job_queue, conclave_config)

        res_mpc = read_rel(conclave_config.input_path + "/" + "actual_mpc_open.csv")
        res_left = read_rel(conclave_config.input_path + "/" + "actual_left.csv")
        res_right = read_rel(conclave_config.input_path + "/" + "actual_right.csv")
        assert len(res_mpc) == 1
        assert len(res_left) == 1
        assert len(res_right) == 1
        res = [[res_mpc[0][0] + res_left[0][0] + res_right[0][0]]]
        write_rel(conclave_config.output_path, "actual_open.csv", res, "1")


def main_mpc(pid: str):
    try:
        use_leaky = sys.argv[2] == "-l"
    except Exception:
        use_leaky = False
    # define name for the workflow
    workflow_name = "real-aspirin-partitioned-" + pid
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
    job_queue = generate_code(protocol_mpc, conclave_config, ["sharemind"], ["python"], apply_optimizations=True)
    dispatch_jobs(job_queue, conclave_config)


if __name__ == "__main__":
    top_pid = sys.argv[1]
    main_mpc(top_pid)
    local_main(top_pid)
