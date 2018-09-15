import sys
from datetime import date

beginning_of_time = date(1970, 1, 1)
input_data_dir = "input_data"
medication_raw_fn = "medication.csv"
diagnosis_raw_fn = "diagnosis.csv"

def convert_date_to_int(date_str):
    month, day, year = [int(s) for s in date_str.strip().split("/")]
    assert year <= 99
    assert month <= 12
    assert day <= 31
    d0 = date(2000 + int(year), int(month), int(day))
    assert d0 > beginning_of_time, "too far in the past: " + str(d0)
    delta = (d0 - beginning_of_time).days
    assert delta <= (1 << 32), "time delta too large: " + str(delta)
    return delta

def convert_meds_row(row):
    return " ".join([
            row[0], # pid*
            "0", # ???
            "0", # year
            "0", # month
            "1" if row[4] == "aspirin" else "2", # prescription*
            "0", # dosage
            "0", # administered
            str(convert_date_to_int(row[7])) # time-stamp*
        ])

def convert_diags_row(row):
    # 2,7,2006,2,1,1,1,1,008.45,1,02/07/06,008.45,008
    converted = ["0" for _ in row]
    converted[0] = row[0] # pid
    converted[8] = "1" if row[8] == "008.45" else "2" # diag
    converted[10] = str(convert_date_to_int(row[10]))
    return converted
    
with open("/".join([input_data_dir, medication_raw_fn])) as meds_in, \
    open("/".join([input_data_dir, diagnosis_raw_fn])) as diag_in:
    meds_rows_in = [convert_meds_row(l.split(",")) for l in meds_in.readlines()]
    diags_rows_in = [convert_diags_row(l.split(",")) for l in diag_in.readlines()]
    print(meds_rows_in)
    print(diags_rows_in)
