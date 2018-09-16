import argparse
import random


def generate_meds_row(distinct_pids, ratio):
    row = ["0" for _ in range(8)]
    patient_id = random.randint(0, distinct_pids)
    med = "aspirin" if random.random() < ratio else "other"
    time_stamp = "01/01/02"  # fixed date for now since it has no influence on performance
    row[0] = str(patient_id)
    row[4] = med
    row[7] = time_stamp
    return ",".join(row)


def generate_diags_row(distinct_pids, ratio):
    row = ["0" for _ in range(13)]
    patient_id = random.randint(0, distinct_pids)
    diagnosis = "414.05" if random.random() < ratio else "other"
    time_stamp = "01/01/01"  # fixed date for now since it has no influence on performance
    row[0] = str(patient_id)
    row[8] = diagnosis
    row[10] = time_stamp
    return ",".join(row)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument("-n", "--num_rows", type=int,
                        help="number of rows per party", required=True)
    parser.add_argument("-d", "--distinct_pids", type=int,
                        help="number of distinct patient IDs", required=True)
    parser.add_argument("-r", "--ratio", type=float,
                        help="ratio of relevant rows", required=True)
    parser.add_argument("-o", "--output", type=str,
                        help="output directory", required=True)
    parser.add_argument("-s", "--seed", type=int,
                        help="random seed", required=False, default=42)

    args = parser.parse_args()
    random.seed(args.seed)

    output_data_dir = args.output
    medication_raw_fn = "medication.csv"
    diagnosis_raw_fn = "diagnosis.csv"

    with open("/".join([output_data_dir, medication_raw_fn]), "w") as meds_out, \
            open("/".join([output_data_dir, diagnosis_raw_fn]), "w") as diag_out:
        print("generating data for {} rows per party with {} distinct IDs and {} ratio".format(
            args.num_rows,
            args.distinct_pids,
            args.ratio
        ))
        medication_rows = [generate_meds_row(args.distinct_pids, args.ratio) for _ in range(args.num_rows)]
        diagnosis_rows = [generate_diags_row(args.distinct_pids, args.ratio) for _ in range(args.num_rows)]

        meds_out.write("\n".join(medication_rows))
        meds_out.write("\n")
        diag_out.write("\n".join(diagnosis_rows))
        diag_out.write("\n")
