#!/bin/bash
if [ $# -ne 3 ]; then
  echo 'usage:' $0 'log_dir start_ts end_ts' 
  exit 1
fi

log_dir=$1
start_ts=$2
end_ts=$3

cd /home/riaps/workspace/dag-placement

source ~/.profile

java  -cp dag-placement.jar edu.vanderbilt.kharesp.dagPlacement.util.Sysstat $log_dir $start_ts $end_ts
