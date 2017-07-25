#!/bin/bash
dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export PYTHONPATH=${dir}

tests=$(ls ${dir}/test/*.py)

for t in ${tests}; do
  python3 ${t}

  if [[ $? -ne 0 ]]; then
    echo "FAILED ${t}"
    exit 1
  fi
done
