#!/bin/bash

if [[ $# -lt 1 ]]; then
  echo "usage: check.sh <input file>"
  exit 1
fi

python3 test.py > correct
python3 sort.py $1 > conclave

if [[ $(diff conclave correct) != "" ]]; then
  echo "ERROR: files are not identical! diff:"
  diff -u conclave correct 
else
  echo "OK -- files are identical!"
fi
