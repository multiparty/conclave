#!/usr/bin/python
from shark import *

if __name__ == "__main__":
    print("start python")
    group_by_keys = read_rel("/mnt/shared/ssn_hybrid/data/group_by_keys.csv")
    sorted_by_key = sort_by(project_indeces(group_by_keys), 1)
    eq_flags = comp_neighs(sorted_by_key, 1)
    write_rel("/mnt/shared/ssn_hybrid/data", "sorted_by_key.csv", sorted_by_key, '"row_index","column_a"')
    write_rel("/mnt/shared/ssn_hybrid/data", "eq_flags.csv", eq_flags, '"column_a"')
    print("done python")
