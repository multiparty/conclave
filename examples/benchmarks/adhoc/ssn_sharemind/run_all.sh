#!/bin/bash
for size in 10 100 1000;
do
   { echo ${size} >> perf_results; time bash docker_run.sh ${size}; } 2>> perf_results;
done