import sys

expected_fn = sys.argv[1]
actual_fn = sys.argv[2]
with open(expected_fn) as f_expected, open(actual_fn) as f_actual:
    expected_raw = f_expected.read().split()[1:]
    assert len(expected_raw) == len(set(expected_raw))
    # strip header
    expected = set(expected_raw)
    actual = set(f_actual.read().split())
    assert expected == actual, "expected " + str(expected) + " but was " + str(actual)
    print("All ok")
