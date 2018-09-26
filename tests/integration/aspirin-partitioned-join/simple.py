import sys
import re
import socket
import struct
import time

INT_SIZE = 4

def write_rel(job_dir, rel_name, rel, schema_header):

    print("Will write to {}/{}".format(job_dir, rel_name))
    path = "{}/{}".format(job_dir, rel_name)
    with open(path, "w") as f:
        # hack header
        f.write(schema_header + "\n")
        for row in rel:
            f.write(",".join([str(val) for val in row]) + "\n")

def read_rel(path_to_rel):

    rows = []
    with open(path_to_rel, "r") as f:
        it = iter(f.readlines())
        for raw_row in it:
            # TODO: only need to do this for first row
            try:
                split_row = [int(val) for val in raw_row.split(",")]
                rows.append([int(val) for val in split_row])
            except ValueError:
                print("skipped header")
    return rows

def project(rel, selected_cols):

    return [[row[idx] for idx in selected_cols] for row in rel]

# TODO handle multi-column case and aggregators other than sum
def aggregate(rel, group_by_idx, over_idx, aggregator):

    acc = {}
    for row in rel:
        key = row[group_by_idx]
        if key not in acc:
            acc[key] = 0
        acc[key] += row[over_idx]
    return [[key, value] for key, value in acc.items()]

def arithmetic_project(rel, target_col_idx, f):

    return [[value if idx != target_col_idx else f(row) for idx, value in enumerate(row)] for row in rel]

def project_indeces(rel):

    return [[idx] + rest for (idx, rest) in enumerate(rel)]

def join_flags(left, right, left_col, right_col):

    return [[int(left_row[left_col] == right_row[right_col])] for left_row in left for right_row in right]

def join(left, right, left_col, right_col):

    left_row_map = dict()
    for left_row in left:
        key = left_row[left_col]
        if key not in left_row_map:
            left_row_map[key] = []
        left_row_map[key].append(left_row)

    joined = []
    for right_row in right:
        right_key = right_row[right_col]
        if right_key in left_row_map:
            left_rows = left_row_map[right_key]
            for left_row in left_rows:
                vals_from_left = [val for (idx, val) in enumerate(left_row) if idx != left_col]
                vals_from_right = [val for (idx, val) in enumerate(right_row) if idx != right_col]
                joined_row = [right_key] + vals_from_left + vals_from_right
                joined.append(joined_row)

    return joined

def index_agg(rel, over_col, distinct_keys, indeces, aggregator):

    empty = 0
    res = [[key[0], empty] for key in distinct_keys]
    for row_idx, key_idx in indeces:
        res[key_idx][1] = aggregator(res[key_idx][1], rel[row_idx][over_col])
    return res

def sort_by(rel, sort_by_col):

    return sorted(rel, key=lambda row: row[sort_by_col])

def comp_neighs(rel, comp_col):

    left = [row[comp_col] for row in rel[0:-1]]
    right = [row[comp_col] for row in rel[1:]]
    return [[int(l == r)] for l, r in zip(left, right)]

def distinct(rel, selected_cols):

    # TODO: general case
    assert len(selected_cols) == 1
    only_selected = project(rel, selected_cols)
    unwrapped = [row[0] for row in only_selected]
    return [[key] for key in set(unwrapped)]

def cc_filter(cond_lambda, rel):

    return list(filter(cond_lambda, rel))

def distinct_count(rel, selected_col):

    return [[len(distinct(rel, [selected_col]))]]

def receive_rel(sock: socket, num_cols: int):
    num_elements_bytes = sock.recv(INT_SIZE)
    num_elements = struct.unpack('i', num_elements_bytes)[0]
    total_size = num_elements * INT_SIZE
    byte_buf = bytearray(total_size)
    view = memoryview(byte_buf)
    while total_size:
        received = sock.recv_into(view, total_size)
        view = view[received:]
        total_size -= received
    element_it = struct.iter_unpack("i", byte_buf)

    rel = []
    col_idx = 0
    row = []
    for el in element_it:
        row.append(el[0])
        col_idx += 1
        if col_idx >= num_cols:
            rel.append(row)
            col_idx = 0
            row = []
    byte_buf = None
    return rel


def send_rel(sock: socket, rel: list):
    num_elements = len(rel) * (len(rel[0]) if rel else 0)
    sock.sendall(struct.pack("i", num_elements))
    as_bytes = bytearray(num_elements * INT_SIZE)
    idx = 0
    for row in rel:
        for el in row:
            struct.pack_into("i", as_bytes, idx, el)
            idx += INT_SIZE
    sock.sendall(as_bytes)


def public_join_as_server(host: str, port: int, rel: list, key_col: int):
    import gc
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((host, port))

    server_socket.listen(5)
    print("server started and listening")
    client_socket, address = server_socket.accept()
    other_rel = receive_rel(client_socket, 1)
    print("done receive")
    my_keys = project_indeces(project(rel, [key_col]))
    print("done project")

    other_keys = project_indeces(other_rel)
    gc.collect()
    print("start join")
    joined = sort_by(join(my_keys, other_keys, 1, 1), 0)
    other_keys = None
    my_keys = None
    print("done join")
    other_cols = project(joined, [2])
    print("done other project")
    send_rel(client_socket, other_cols)
    other_cols = None
    print("done send")
    res_rel = []
    for idx in joined:
        res_rel.append(rel[idx[1]])
    server_socket.close()
    return res_rel


def public_join_as_client(host: str, port: int, rel: list, key_col: int):
    sock = None
    connected = False
    while not connected:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((host, port))
            connected = True
        except Exception as e:
            print(e)
            connected = False
            time.sleep(.042)
    send_rel(sock, project(rel, [key_col]))
    rel_back = receive_rel(sock, 1)
    res_rel = []
    for idx in rel_back:
        res_rel.append(rel[idx[0]])
    sock.close()
    #num_cols = len(rel[0]) if rel else 0
    #cols_to_keep = [i for i in range(num_cols) if i != key_col]
    return res_rel
    #return project(res_rel, cols_to_keep)

def key_union(left: list, right: list, l: int, r: int):
    keys = set()
    for row in left:
        keys.add(row[l])
    for row in right:
        keys.add(row[r])
    return keys


def filter_by_keys(rel: list, keys: set, key_col: int):
    return [row for row in rel if row[key_col] in keys]


def construct_index_rel(keys: list, other_keys: list, dist_keys: set):
    keys = [[idx, 0, key] for (idx, key) in filter_by_keys(project_indeces(keys), dist_keys, 1)]
    other_keys = [[idx, 1, key] for (idx, key) in filter_by_keys(project_indeces(other_keys), dist_keys, 1)]
    return keys + other_keys


def reconstruct(
        left_rel: list,
        right_rel: list,
        left_col: int,
        right_col: int,
        idx_rel: list,
        me: int):
    res_rel = []
    left_dummy = [1 for _ in range(len(left_rel[0]))]
    right_dummy = [1 for _ in range(len(right_rel[0]))]
    for left_idx, left_own, right_idx, right_own in idx_rel:
        left_side = left_rel[left_idx] if left_own == me else left_dummy
        right_side = right_rel[right_idx] if right_own == me else right_dummy
        right_key = right_side[right_col]

        from_left = [val for (idx, val) in enumerate(left_side) if idx != left_col]
        from_right = [val for (idx, val) in enumerate(right_side) if idx != right_col]

        joined_row = [right_key] + from_left + from_right
        res_rel.append(joined_row)
    return res_rel


def public_join_as_server_part(
        host: str,
        port: int,
        my_left_rel: list,
        my_right_rel: list,
        left_key_col: int,
        right_key_col: int):
    import gc
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((host, port))

    server_socket.listen(1)
    print("server started and listening")
    client_socket, address = server_socket.accept()

    other_left_keys = receive_rel(client_socket, 1)
    other_right_keys = receive_rel(client_socket, 1)
    print("done receive")

    dist_keys = key_union(my_left_rel, my_right_rel, left_key_col, right_key_col) \
                & key_union(other_left_keys, other_right_keys, 0, 0)

    left = construct_index_rel(project(my_left_rel, [left_key_col]), other_left_keys, dist_keys)
    right = construct_index_rel(project(my_right_rel, [right_key_col]), other_right_keys, dist_keys)
    dist_keys = None

    print("done project")

    gc.collect()
    print("start join")
    joined = sort_by(join(left, right, 2, 2), 0)
    print("done join")
    idx_rel = project(joined, [1, 2, 3, 4])
    joined = None
    gc.collect()
    print("done other project")
    send_rel(client_socket, idx_rel)
    print("done send")
    res_rel = reconstruct(my_left_rel, my_right_rel, left_key_col, right_key_col, idx_rel, me=0)
    server_socket.close()
    return res_rel


def public_join_as_client_part(
        host: str,
        port: int,
        left_rel: list,
        right_rel: list,
        left_key_col: int,
        right_key_col: int):
    sock = None
    connected = False
    while not connected:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((host, port))
            connected = True
        except Exception as e:
            print(e)
            connected = False
            time.sleep(.042)
    send_rel(sock, project(left_rel, [left_key_col]))
    send_rel(sock, project(right_rel, [right_key_col]))
    idx_rel = receive_rel(sock, 4)
    sock.close()
    return reconstruct(left_rel, right_rel, left_key_col, right_key_col, idx_rel, me=1)


def pub_join(host: str, port: int, is_server: bool, rel: list, key_col: int):
    if is_server:
        return public_join_as_server(host, port, rel, key_col)
    else:
        return public_join_as_client(host, port, rel, key_col)


def pub_join_part(host: str, port: int, is_server: bool, rel: list, other_rel: list, key_col: int):
    if is_server:
        return public_join_as_server_part(host, port, rel, other_rel, key_col, key_col)
    else:
        return public_join_as_client_part(host, port, rel, other_rel, key_col, key_col)

if __name__ == "__main__":
    data_root = "/home/sharemind/Desktop/conclave/tests/integration/aspirin-partitioned-join/data/"
    
    left_diagnosis = read_rel(data_root + "left_diagnosis.csv")
    left_diagnosis_proj = project(left_diagnosis, [0, 8, 10])
    left_medication = read_rel(data_root + "left_medication.csv")
    left_medication_proj = project(left_medication, [0, 4, 7])

    right_diagnosis = read_rel(data_root + "right_diagnosis.csv")
    right_diagnosis_proj = project(right_diagnosis, [0, 8, 10])
    right_medication = read_rel(data_root + "right_medication.csv")
    right_medication_proj = project(right_medication, [0, 4, 7])

    keys_left = key_union(left_diagnosis_proj, left_medication_proj, 0, 0)
    keys_right = key_union(right_diagnosis_proj, right_medication_proj, 0, 0)
    keys = keys_left & keys_right

    meds = filter_by_keys(left_medication_proj + right_medication_proj, keys, 0)
    diags = filter_by_keys(left_diagnosis_proj + right_diagnosis_proj, keys, 0)
    
    heart_patients = cc_filter(lambda row : row[1] == 1, diags)
    aspirin = cc_filter(lambda row : row[1] == 1, meds)
    joined  = join(aspirin, heart_patients, 0, 0)
    cases = cc_filter(lambda row : row[4] < row[2], joined)
    expected = distinct_count(cases, 0)
    write_rel(data_root, "expected.csv", expected, '"0"')
