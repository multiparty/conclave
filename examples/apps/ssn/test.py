
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

def join(left, right, left_col, right_col):
    
    joined = []
    for left_row in left:
        for right_row in right:
            if left_row[left_col] == right_row[right_col]:
                vals_from_left = [val for (idx, val) in enumerate(left_row) if idx != left_col]
                vals_from_right = [val for (idx, val) in enumerate(right_row) if idx != right_col]
                joined_row = [left_row[left_col]] + vals_from_left + vals_from_right
                joined.append(joined_row)
    return joined

def agg(rel, group_by, over):

	aggs = {}
	for row in rel:
		key = row[group_by]
		if key not in aggs:
			aggs[key] = 0
		aggs[key] = aggs[key] + row[over]
	return sorted([str(k) + "," + str(v) for k, v in aggs.items()])

root = "/mnt/shared"

left = read_rel("/mnt/shared/ssn-data/govreg.csv")
right = read_rel("/mnt/shared/ssn-data/company0.csv") + read_rel("/mnt/shared/ssn-data/company1.csv")
joined = join(left, right, 0, 0)
agged = agg(joined, 1, 2)
print(agged)
