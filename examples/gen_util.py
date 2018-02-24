import os
import random
import sys
from random import randint


def generate(out_path, num_cols, num_rows, input_range, seed):
    random.seed(seed)
    num_cols = int(num_cols)
    num_rows = int(num_rows)

    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    with open(out_path, 'w') as f:
        for j in range(num_rows):
            f.write(','.join([str(randint(1, int(input_range))) for _ in range(num_cols)]))
            f.write("\n")


if __name__ == "__main__":
    generate(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
