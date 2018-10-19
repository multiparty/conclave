import os
import random
import sys
from random import randint


def generate(out_path: str, num_rows: int, party: int, col_names: str):
    random.seed(5)
    num_rows = int(num_rows)

    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    with open(out_path, "w+") as f:
        if col_names is not None:
            f.write(col_names + "\n")
        for j in range(num_rows):
            f.write(",".join([party, str(randint(1, 10))]))
            f.write("\n")


if __name__ == "__main__":
    try:
        col_names = sys.argv[4]
    except Exception:
        col_names = None
    generate(sys.argv[1], sys.argv[2], sys.argv[3], col_names)
