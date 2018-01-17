#!/bin/bash
export PYTHONPATH=${PWD}/..

python3 codegen/test_python.py
python3 codegen/test_sharemind.py
python3 codegen/test_spark.py
python3 dag_rewrite/test_comp_dag_rewrite.py
python3 dag_rewrite/test_simple_dag_rewrite.py
python3 partition_dag/test_partition_dag.py
