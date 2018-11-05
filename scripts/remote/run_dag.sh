#!/bin/bash
if [ $# -ne 5 ]; then
  echo 'usage:' $0 'graph_id graph_description_path publication_rate time_interval log_dir' 
  exit 1
fi

graph_id=$1
graph_description=$2
publication_rate=$3
time_interval=$4
log_dir=$5

mkdir -p $log_dir/$graph_id/err
mkdir -p $log_dir/$graph_id/dag

{
  read 
  while read -r line
  do
    vid="$(cut -d';' -f1 <<<$line)"
    java -Dlog4j.configurationFile=log4j2.xml -cp ./build/libs/dag-placement.jar edu.vanderbilt.kharesp.dagPlacement.Vertex $graph_id $line $publication_rate $time_interval $log_dir/$graph_id/dag 1>$log_dir/$graph_id/err/"$vid".log 2>&1  & 
  done 
}< "$graph_description"

wait
