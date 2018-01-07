import os

import salmon.lang as lang
from salmon import generate_and_dispatch
from salmon.config import CodeGenConfig
from salmon.utils import defCol


def protocol():
    """
    A demo protocol which reads data from data/input_relation.csv, computes a multiplication, followed by an aggregation,
    and stores the result under data/aggregated.csv.
    :return set of input relations
    """
    # define the input schema, providing column name, type, and trust set
    input_columns = [
        defCol("column_a", "INTEGER", [1]),
        defCol("column_b", "INTEGER", [1])
    ]
    # define input relation, providing relation name, columns, and owner set
    input_relation = lang.create("input_relation", input_columns, {1})
    # square column_b, i.e., compute (column_a, column_b) -> (column_a, column_b * column_b)
    squared = lang.multiply(input_relation, "squared", "column_b", ["column_b", "column_b"])
    # sum group by column_a on column_b and rename group-over column to summed
    aggregated = lang.aggregate(squared, "aggregated", ["column_a"], "column_b", "+", "summed")
    # leaf nodes are automatically written to file so aggregated will be written to ./data/aggregated.csv

    # return all input relations
    return {input_relation}


if __name__ == "__main__":
    # define name for the workflow
    workflow_name = "python-demo"
    # configure conclave
    conclave_config = CodeGenConfig(workflow_name)
    # need the absolute path to current directory
    current_dir = os.path.dirname(os.path.realpath(__file__))
    # point conclave to the directory where the generated code should be stored/ read from
    conclave_config.code_path = os.path.join(current_dir, workflow_name)
    # point conclave to directory where data is to be read from...
    conclave_config.input_path = os.path.join(current_dir, "data")
    # and written to
    conclave_config.output_path = os.path.join(current_dir, "data")
    # define this party's unique ID (in this demo there is only one party)
    conclave_config.pid = 1
    # define all parties involved in this workflow
    conclave_config.all_pids = [1]
    # compile and execute protocol, specifying available mpc and local processing backends
    generate_and_dispatch(protocol, conclave_config, ["sharemind"], ["python"])
