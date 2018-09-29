import argparse
import random

from datetime import date

import os

beginning_of_time = date(1970, 1, 1)


def convert_date_to_int(date_str: str):
    month, day, year = [int(s) for s in date_str.strip().split("/")]
    assert year <= 99
    assert month <= 12
    assert day <= 31
    d0 = date(2000 + int(year), int(month), int(day))
    assert d0 > beginning_of_time, "too far in the past: " + str(d0)
    delta = (d0 - beginning_of_time).days
    assert delta <= (1 << 32), "time delta too large: " + str(delta)
    return delta


def convert_meds_row(row: list):
    return ",".join([
        row[0],  # pid*
        "0",  # ???
        "0",  # year
        "0",  # month
        "1" if ("aspirin" in row[4].lower()) else "2",  # prescription*
        "0",  # dosage
        "0",  # administered
        str(convert_date_to_int(row[7]))  # time-stamp*
    ])


def convert_diags_row(row: list):
    converted = ["0" for _ in row]
    converted[0] = row[0]  # pid
    converted[8] = "1" if row[8][0:3] == "414" else "2"  # diag
    converted[10] = str(convert_date_to_int(row[10]))
    return ",".join(converted)


def generate_meds_row(distinct_pids, ratio):
    row = ["0" for _ in range(8)]
    patient_id = random.randint(0, distinct_pids)
    med = "aspirin" if random.random() < ratio else "other"
    time_stamp = "01/01/02"  # fixed date for now since it has no influence on performance
    row[0] = str(patient_id)
    row[4] = med
    row[7] = time_stamp
    # pre-convert
    return convert_meds_row(row)


def generate_diags_row(distinct_pids, ratio):
    row = ["0" for _ in range(13)]
    patient_id = random.randint(0, distinct_pids)
    diagnosis = "414.05" if random.random() < ratio else "other"
    time_stamp = "01/01/01"  # fixed date for now since it has no influence on performance
    row[0] = str(patient_id)
    row[8] = diagnosis
    row[10] = time_stamp
    # pre-convert
    return convert_diags_row(row)


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
    os.makedirs(os.path.dirname(output_data_dir), exist_ok=True)

    with open("/".join([output_data_dir, medication_raw_fn]), "w+") as meds_out, \
            open("/".join([output_data_dir, diagnosis_raw_fn]), "w+") as diag_out:
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
