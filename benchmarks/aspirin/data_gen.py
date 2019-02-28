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


def generate_meds_row(pids_low: int, pids_high: int, ratio: int, convert: bool = True):
    row = ["0" for _ in range(8)]
    patient_id = random.randint(pids_low, pids_high)
    med = "aspirin"
    time_stamp = "01/01/02"  # fixed date for now since it has no influence on performance
    row[0] = str(patient_id)
    row[1] = "7"
    row[2] = "2006"
    row[3] = "9"
    row[4] = med
    row[5] = "10 mg"
    row[6] = "oral"
    row[7] = time_stamp
    # pre-convert
    if convert:
        return convert_meds_row(row)
    else:
        return ",".join(row)


def generate_diags_row(pids_low: int, pids_high: int, ratio: int, convert: bool = True):
    row = ["0" for _ in range(13)]
    patient_id = random.randint(pids_low, pids_high)
    diagnosis = "414.05"
    time_stamp = "01/01/01"  # fixed date for now since it has no influence on performance
    row[0] = str(patient_id)
    row[1] = "7"
    row[2] = "2006"
    row[3] = "2"
    row[4] = "1"
    row[5] = "1"
    row[6] = "1"
    row[7] = "1"
    row[8] = diagnosis
    row[9] = "1"
    row[10] = time_stamp
    row[11] = diagnosis
    row[12] = diagnosis
    # pre-convert
    if convert:
        return convert_diags_row(row)
    else:
        return ",".join(row)


def generate_data(args, gen, fn):
    chunk_size = 1000000
    with open("/".join([output_data_dir, fn]), "w") as out:
        print("generating {} data for {} rows with {} distinct IDs, between {} and {} with {} ratio and seed {}".format(
            args.mode,
            args.num_rows,
            args.distinct_pids,
            args.low,
            args.upper,
            args.ratio,
            args.seed
        ))
        num_chunks = int(math.ceil(args.num_rows / chunk_size))  # might be unnecessary
        rows_left = args.num_rows
        for c in range(num_chunks):
            num_to_gen = min(rows_left, chunk_size)
            print("left to generate", rows_left)
            rows = [gen(args.low, args.upper, args.ratio, not args.smcql) for _ in range(num_to_gen)]
            out.write("\n".join(rows))
            out.write("\n")
            rows_left -= num_to_gen


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument("-n", "--num_rows", type=int,
                        help="number of rows per party", required=True)
    parser.add_argument("-l", "--low", type=int,
                        help="lower bound on patient ID value", required=True)
    parser.add_argument("-u", "--upper", type=int,
                        help="upper bound on patient ID value", required=True)
    parser.add_argument("-r", "--ratio", type=float,
                        help="ratio of relevant rows", required=True)
    parser.add_argument("-o", "--output", type=str,
                        help="output directory", required=True)
    parser.add_argument("-f", "--file_name", type=str,
                        help="output file name", required=True)
    parser.add_argument("-m", "--mode", type=str,
                        help="mode (medication or diagnosis)", required=True)
    parser.add_argument("-s", "--seed", type=int,
                        help="random seed", required=False, default=42)
    parser.add_argument("-q", "--smcql", action='store_true',
                        help="use smcql format")

    args = parser.parse_args()
    random.seed(args.seed)

    output_data_dir = args.output

    low = args.low
    high = args.upper
    args.distinct_pids = high - low

    os.makedirs(os.path.dirname(output_data_dir), exist_ok=True)
    if args.mode == "medication":
        generate_data(args, generate_meds_row, args.file_name)
    elif args.mode == "diagnosis":
        generate_data(args, generate_diags_row, args.file_name)
    else:
        raise "Unknown mode " + str(args.mode)
