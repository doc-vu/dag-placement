#!/bin/bash
if [ $# -ne 5 ]; then
  echo 'usage:' $0 'graph_id vertex_description execution_interval log_dir zmq' 
  exit 1
fi

graph_id=$1
vertex_description="${2//\\/}"
execution_interval=$3
log_dir=$4
zmq=$5

cd /home/shweta/workspace/research/dag-placement

mkdir -p $log_dir/err
mkdir -p $log_dir/dag
vid="$(cut -d';' -f1 <<<$vertex_description)"

source ~/.profile

if [ "$zmq" -eq 1 ];
then
  java -Dlog4j.configurationFile=log4j2.xml  -cp build/libs/dag-placement.jar edu.vanderbilt.kharesp.dagPlacement.zmq.Vertex $graph_id $vertex_description $execution_interval $log_dir/dag  1>$log_dir/err/"$vid".log 2>&1
else
  java -Dlog4j.configurationFile=log4j2.xml  -cp build/libs/dag-placement.jar edu.vanderbilt.kharesp.dagPlacement.dds.Vertex $graph_id $vertex_description $execution_interval $log_dir/dag  1>$log_dir/err/"$vid".log 2>&1
fi
