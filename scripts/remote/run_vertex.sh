#!/bin/bash
if [ $# -ne 6 ]; then
  echo 'usage:' $0 'graph_id vertex_description publication_rate execution_interval log_dir processing_interval' 
  exit 1
fi

graph_id=$1
vertex_description=$2
publication_rate=$3
execution_interval=$4
log_dir=$5
processing_interval=$6

mkdir -p $log_dir/$graph_id/err
mkdir -p $log_dir/$graph_id/dag
vid="$(cut -d';' -f1 <<<$vertex_description)"

java -Dlog4j.configurationFile=log4j2.xml  -cp ./build/libs/dag-placement.jar edu.vanderbilt.kharesp.dagPlacement.Vertex $graph_id $vertex_description $publication_rate $execution_interval $log_dir/$graph_id/dag $processing_interval 1>$log_dir/$graph_id/err/"$vid".log 2>&1 
