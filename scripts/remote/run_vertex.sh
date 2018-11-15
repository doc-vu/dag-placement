#!/bin/bash
if [ $# -ne 7 ]; then
  echo 'usage:' $0 'graph_id vertex_description publication_rate execution_interval log_dir processing_interval zmq' 
  exit 1
fi

graph_id=$1
vertex_description="${2//\\/}"
publication_rate=$3
execution_interval=$4
log_dir=$5
processing_interval=$6
zmq=$7

cd /home/riaps/workspace/dag-placement

mkdir -p $log_dir/err
mkdir -p $log_dir/dag
vid="$(cut -d';' -f1 <<<$vertex_description)"

source ~/.profile

if [ "$zmq" -eq 1 ];
then
  java -Dlog4j.configurationFile=log4j2.xml  -cp dag-placement.jar edu.vanderbilt.kharesp.dagPlacement.zmq.Vertex $graph_id $vertex_description $publication_rate $execution_interval $log_dir/dag $processing_interval 1>$log_dir/err/"$vid".log 2>&1
else
  java -Dlog4j.configurationFile=log4j2.xml  -cp dag-placement.jar edu.vanderbilt.kharesp.dagPlacement.dds.Vertex $graph_id $vertex_description $publication_rate $execution_interval $log_dir/dag $processing_interval 1>$log_dir/err/"$vid".log 2>&1
fi
