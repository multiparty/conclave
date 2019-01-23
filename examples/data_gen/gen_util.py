import os
import random
import sys
from random import randint


def generate(out_path, num_cols, num_rows, input_range, seed, col_names, use_random):
    random.seed(seed)
    num_cols = int(num_cols)
    num_rows = int(num_rows)
    use_random = True if use_random == "1" else False

    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    with open(out_path, "w+") as f:
        f.write(col_names + "\n")
        for j in range(num_rows):
            if use_random:
                f.write(",".join([str(randint(1, int(input_range))) for _ in range(num_cols)]))
            else:
                f.write(",".join([str(j) for _ in range(num_cols)]))
            f.write("\n")


if __name__ == "__main__":
    generate(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], 5, sys.argv[5], "1")
