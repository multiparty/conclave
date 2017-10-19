import sys, os
from random import randint


# Some adhoc utility functions

def get_sharemind_config(pid, local=False):

    if local:
        return {
            "pid": pid,
            "parties": {
                1: {"host": "localhost", "port": 9001},
                2: {"host": "localhost", "port": 9002},
                3: {"host": "localhost", "port": 9003}
            }
        }
    else:
        return {
            "pid": pid,
            "parties": {
                1: {"host": "ca-spark-node-0", "port": 9001},
                2: {"host": "cb-spark-node-0", "port": 9002},
                3: {"host": "cc-spark-node-0", "port": 9003}
            }
        }


def generate_data(pid, root_dir):

    data = {
        1: '''"a","b"
42,42
2,200
3,300
4,400
5,500
6,600
7,700
7,800
7,900
8,1000
9,1100''',
        2: '''"c","d"
42,1001
2,2001
3,3001
4,4001
5,5001
6,6001
7,7001
8,8001
9,9001
10,10001
10,11001''',
        3: '''"e","f"
1,1
'''
    }

    with open(root_dir + "/" + "in" + str(pid) + ".csv", "w") as f:
        f.write(data[pid])

def generate_agg_data(pid, root_dir):

    data = {
        1: '''"a","b"
2,42
2,200
3,300
1,400
5,500
1,600
7,700
7,800
7,900
1,1000
9,1100''',
        2: '''"c","d"
1,1''',
        3: '''"e","f"
1,1
'''
    }

    with open(root_dir + "/" + "in" + str(pid) + ".csv", "w") as f:
        f.write(data[pid])



def check_res(expected, res_path):

    with open(res_path, "r") as f:
        actual = sorted(f.read().split("\n"))
        assert expected == actual, actual


def generate_input(out_path, num_cols, num_rows, col_names=''):
    num_cols = int(num_cols)
    num_rows = int(num_rows)

    with_col_names = False
    if col_names != '':
        with_col_names = True

    if with_col_names:
        assert (len(col_names) == num_cols), \
            'Number of columns is unequal to number of column names.'

    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    with open(out_path, 'w') as f:
        if with_col_names:
            f.write(','.join(col_names) + '\n')
        f.write('\n'.join([','.join([str(randint(0,9))
                                     for i in range(num_cols)])
                           for j in range(num_rows)]))
