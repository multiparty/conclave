#!/usr/bin/python
from shark import *


def process_rel(rel, rel_name_prefix, length):
    write_rel("/mnt/shared/ssn_hybrid/data", rel_name_prefix + "_indexes.csv", rel, "idx")
    original_ordering = get_original_ordering(rel)
    encoded = encode_as_flags(project(original_ordering, [1]), length)
    write_rel("/mnt/shared/ssn_hybrid/data", rel_name_prefix + "_encoded.csv", encoded, "idx")
    write_rel("/mnt/shared/ssn_hybrid/data", rel_name_prefix + "_original_ordering.csv",
              project(original_ordering, [0]),
              "idx")


if __name__ == "__main__":
    print("start python")
    in1_keys = read_rel('/mnt/shared/ssn_hybrid/data/in1_keys.csv')
    in1_keys_indexed = project_indeces(in1_keys)
    in2_keys = read_rel('/mnt/shared/ssn_hybrid/data/in2_keys.csv')
    in2_keys_indexed = project_indeces(in2_keys)
    joined_indexes = join(in1_keys_indexed, in2_keys_indexed, 1, 1)
    in1_indexes = project(joined_indexes, [1])
    in2_indexes = project(joined_indexes, [2])
    process_rel(in1_indexes, "in1", len(in1_keys))
    process_rel(in2_indexes, "in2", len(in2_keys))
    assert len(in1_indexes) == len(in2_indexes)
    write_rel("/mnt/shared/ssn_hybrid/data", "num_lookups.csv", [[len(in1_indexes)]], "idx")
    print("done python")
