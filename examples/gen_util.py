import sys, os
import random
from random import randint
random.seed(42)


def generate(out_path, num_cols, num_rows, num_inpts, col_names=None):
    num_cols = int(num_cols)
    num_rows = int(num_rows)

    with_col_names = False
    if col_names is not None:
        with_col_names = True
        col_names = col_names.split(',')
        assert (len(col_names) == num_cols), \
            'Number of columns is unequal to number of column names.'

    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    with open(out_path, 'w') as f:
        if with_col_names:
            f.write(','.join(col_names) + '\n')
        for j in range(num_rows):
            f.write(','.join([str(randint(0,int(num_inpts))) for i in range(num_cols)]))
            f.write("\n")

if __name__ == "__main__":

    try:
        generate(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
    except IndexError:
        generate(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])


