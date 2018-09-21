import sys
from datetime import date


beginning_of_time = date(1970, 1, 1)

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
    return ",".join([
            row[0], # pid*
            "0", # ???
            "0", # year
            "0", # month
            "1" if ("aspirin" in row[4].lower()) else "2", # prescription*
            "0", # dosage
            "0", # administered
            str(convert_date_to_int(row[7])) # time-stamp*
        ])

def convert_diags_row(row):
    converted = ["0" for _ in row]
    converted[0] = row[0] # pid
    converted[8] = "1" if row[8][0:3] == "414" else "2" # diag
    converted[10] = str(convert_date_to_int(row[10]))
    return ",".join(converted)

if __name__ == '__main__':
    input_data_dir = sys.argv[1] 
    output_data_dir = sys.argv[2] 
    medication_raw_fn = "medication.csv"
    diagnosis_raw_fn = "diagnosis.csv"
    
    with open("/".join([input_data_dir, medication_raw_fn])) as meds_in, \
        open("/".join([input_data_dir, diagnosis_raw_fn])) as diag_in, \
        open("/".join([output_data_dir, medication_raw_fn]), "w") as meds_out, \
        open("/".join([output_data_dir, diagnosis_raw_fn]), "w") as diag_out :
        print("converting data")
        # read and convert
        meds_converted = [convert_meds_row(l.split(",")) for l in meds_in.readlines()]
        diags_converted = [convert_diags_row(l.split(",")) for l in diag_in.readlines()]
        # write out
        meds_out.write("\n".join(meds_converted))
        meds_out.write("\n")
        diag_out.write("\n".join(diags_converted))
        diag_out.write("\n")
