with open("data/expected.csv") as f_expected, open("data/actual_obl_open.csv") as f_actual:
    expected = set(f_expected.read().split())
    actual = set(f_actual.read().split())
    assert expected == actual, "expected " + str(expected) + " but was " + str(actual)
    print("All ok")
