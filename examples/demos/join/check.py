with open("data/expected.csv") as f_expected, open("data/actual.csv") as f_actual:
    expected = set(f_expected.read().split())
    actual = set(f_actual.read().split())
    assert expected == actual
    print("All ok")
    