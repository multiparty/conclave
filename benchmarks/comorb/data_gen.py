import argparse
import math
import os
import random
from datetime import date

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


def convert_diags_row(row: list):
    converted = ["0" for _ in row]
    converted[0] = row[0]  # pid
    converted[10] = str(convert_date_to_int(row[10]))
    converted[12] = row[12]
    return ",".join(converted)


def generate_diags_row(num_rows: int, diagnosis_range: int, convert: bool = True):
    row = ["0" for _ in range(13)]
    patient_id = random.randint(1, num_rows + 1)
    diagnosis = random.randint(1, diagnosis_range + 1)
    time_stamp = "01/01/01"  # fixed date for now since it has no influence on performance
    row[0] = str(patient_id)
    row[1] = "7"
    row[2] = "2006"
    row[3] = "2"
    row[4] = "1"
    row[5] = "1"
    row[6] = "1"
    row[7] = "1"
    row[8] = str(diagnosis)
    row[9] = "1"
    row[10] = time_stamp
    row[11] = str(diagnosis)
    row[12] = str(diagnosis)
    # pre-convert
    if convert:
        return convert_diags_row(row)
    else:
        return ",".join(row)


def generate_data(args, fn):
    chunk_size = 1000000
    with open("/".join([output_data_dir, fn]), "w") as out:
        format_str = "generating diagnosis data for {} rows with {} distinct codes and seed {}"
        print(format_str.format(args.num_rows, args.diagnosis_range, args.seed))
        num_chunks = int(math.ceil(args.num_rows / chunk_size))  # might be unnecessary
        rows_left = args.num_rows
        for c in range(num_chunks):
            num_to_gen = min(rows_left, chunk_size)
            print("left to generate", rows_left)
            rows = [generate_diags_row(args.num_rows, args.diagnosis_range, not args.smcql) for _ in range(num_to_gen)]
            out.write("\n".join(rows))
            out.write("\n")
            rows_left -= num_to_gen


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument("-n", "--num_rows", type=int,
                        help="number of rows per party", required=True)
    parser.add_argument("-d", "--diagnosis_range", type=int,
                        help="number of distinct diagnosis codes", required=True)
    parser.add_argument("-o", "--output", type=str,
                        help="output directory", required=True)
    parser.add_argument("-f", "--file_name", type=str,
                        help="output file name", required=True)
    parser.add_argument("-s", "--seed", type=int,
                        help="random seed", required=False, default=42)
    parser.add_argument("-q", "--smcql", action='store_true',
                        help="use smcql format")

    args = parser.parse_args()
    random.seed(args.seed)

    output_data_dir = args.output

    os.makedirs(os.path.dirname(output_data_dir), exist_ok=True)
    generate_data(args, args.file_name)
