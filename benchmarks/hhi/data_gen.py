import argparse
import os
import random


def generate_date(output_data_dir: str, fn: str, num_rows: int, party_id: int):
    outpath = "/".join([output_data_dir, fn])
    with open(outpath, "w") as f:
        f.write('"companyID","price"\n')
        for _ in range(num_rows):
            row = [str(party_id), str(random.randint(0, 200))]
            f.write(",".join(row) + "\n")


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("-n", "--num_rows", type=int,
                        help="number of rows per party", required=True)
    parser.add_argument("-p", "--party", type=int,
                        help="ID of party to generate for", required=True)
    parser.add_argument("-o", "--output", type=str,
                        help="output directory", required=True)
    parser.add_argument("-f", "--file_name", type=str,
                        help="output file name", required=True)
    parser.add_argument("-s", "--seed", type=int,
                        help="random seed", required=False, default=42)

    args = parser.parse_args()

    output_data_dir = args.output
    random.seed(args.seed)

    os.makedirs(os.path.dirname(output_data_dir), exist_ok=True)
    generate_date(args.output, args.file_name, args.num_rows, args.party)


if __name__ == "__main__":
    main()
