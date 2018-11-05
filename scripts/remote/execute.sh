#!/bin/bash

for rate in 10 100;
do
  for v in `seq 4 8`;
  do
    for g in `seq 1 5`;
    do 
      echo 'DAG type: Erdos Renyi- Executing test for v:'$v 'g:'$g 'for rate:'$rate
      mkdir -p log/erdos_renyi/rate$rate/v$v/g$g
      ./scripts/remote/run_dag.sh 'v_'$v'_g_'$g'_c1' dags/config/input_rate/erdos_renyi/v$v/g$g/v_$v'_g_'$g'_c1.txt' $rate 300 log/erdos_renyi/rate$rate/v$v/g$g
    done 
  done 
done
