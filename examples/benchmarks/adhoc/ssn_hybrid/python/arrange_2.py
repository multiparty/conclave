#!/usr/bin/python
from shark import *


def process_rel(rel_name_prefix):
    lookups = read_rel("/mnt/shared/ssn_hybrid/data/" + rel_name_prefix + "_indexes.csv")
    indexes_and_flags = read_rel("/mnt/shared/ssn_hybrid/data/" + rel_name_prefix + "_indexes_and_flags.csv")
    shark_flags = compute_shark_flags(lookups, indexes_and_flags)
    shark_indexes = project(shark_flags, [0])
    write_rel("/mnt/shared/ssn_hybrid/data", rel_name_prefix + "_shark_indexes.csv", shark_indexes, "idx")


if __name__ == "__main__":
    print("start python")
    process_rel("in1")
    process_rel("in2")
    print("done python")
