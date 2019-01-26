import sys

expected_fn = sys.argv[1]
actual_fn = sys.argv[2]
with open(expected_fn) as f_expected, open(actual_fn) as f_actual:
    # strip header
    expected = f_expected.read().split()[1:]
    actual = f_actual.read().split()
    expected_values = [row.split(",")[1] for row in expected]
    actual_values = [row.split(",")[1] for row in expected]
    assert set(expected) == set(actual), "expected " + str(expected) + " but was " + str(actual)
    assert expected_values == actual_values, "expected " + str(expected_values) + " but was " + str(actual_values)
    print("All ok")
