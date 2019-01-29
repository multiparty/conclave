import os
import sys


def check(expected, actual):
    expected_values = [row.split(",") for row in expected]
    actual_values = [row.split(",") for row in expected]
    assert set(expected) == set(actual), "expected " + str(expected) + " but was " + str(actual)
    assert expected_values == actual_values, "expected " + str(expected_values) + " but was " + str(actual_values)


expected_fn = sys.argv[1]
actual_fn = sys.argv[2]
# TODO hack hack hack
current_dir = os.path.dirname(os.path.realpath(__file__))
with open(expected_fn) as f_expected, open(current_dir + "/data/actual_1.csv") as f_actual, open(
        current_dir + "/data/actual_2.csv") as f_actual_2:
    # strip header
    expected = f_expected.read().split()[1:]
    actual_1 = f_actual.read().split()[1:]
    actual_2 = f_actual_2.read().split()[1:]
    check(expected, actual_1)
    check(expected, actual_2)
    print("All ok")
