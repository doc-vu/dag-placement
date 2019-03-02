#!/bin/bash
if [ $# -ne 6 ]; then
  echo 'usage:' $0 'vertex_description execution_interval log_dir zmq zk_connector domain_id' 
  exit 1
fi

vertex_description="${1//\\/}"
execution_interval=$2
log_dir=$3
zmq=$4
zk_connector=$5
domain_id=$6

cd /home/riaps/workspace/dag-placement 

graph_id="$(cut -d';' -f1 <<<$vertex_description)"
vid="$(cut -d';' -f2 <<<$vertex_description)"
vertex_description_wo_gid="$( cut -d ';' -f 2- <<< "$vertex_description" )"

mkdir -p $log_dir/$graph_id/err
mkdir -p $log_dir/$graph_id/dag

source ~/.profile

if [ "$zmq" -eq 1 ];
then
  java -Dlog4j.configurationFile=log4j2.xml  -cp dag-placement.jar edu.vanderbilt.kharesp.dagPlacement.zmq.Vertex $graph_id $vertex_description_wo_gid $execution_interval $log_dir/$graph_id/dag $zk_connector 1>$log_dir/$graph_id/err/"$vid".log 2>&1
else
  java -Dlog4j.configurationFile=log4j2.xml  -cp dag-placement.jar edu.vanderbilt.kharesp.dagPlacement.dds.Vertex $graph_id $vertex_description_wo_gid $execution_interval $log_dir/$graph_id/dag $zk_connector $domain_id  1>$log_dir/$graph_id/err/"$vid".log 2>&1
fi

sleep 10
