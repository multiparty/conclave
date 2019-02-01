import sys

expected_fn = sys.argv[1]
actual_fn = sys.argv[2]
with open(expected_fn) as f_expected, open(actual_fn) as f_actual:
    # strip header
    expected = set(f_expected.read().split()[1:])
    actual = set(f_actual.read().split()[1:])
    assert expected == actual, "expected " + str(expected) + " but was " + str(actual)
    print("All ok")
