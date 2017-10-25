#!/bin/bash
python3 test.py > correct
python3 sort.py /mnt/shared/ssn-1/sharemind-7/aggopened.csv > conclave

if [[ $(diff conclave correct) -ne "" ]]; then
  echo "ERROR: files are not identical! diff:"
  diff -u conclave correct 
else
  echo "OK -- files are identical!"
fi
