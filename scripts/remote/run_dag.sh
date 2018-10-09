#!/bin/bash
if [ $# -ne 2 ]; then
  echo 'usage:' $0 'graph_id graph_description_path' 
  exit 1
fi

graph_id=$1
graph_description=$2

mkdir -p ./log/err/$graph_id 

{
  read 
  while read -r line
  do
    vid="$(cut -d';' -f1 <<<$line)"
    ( ( nohup java -Dlog4j.configurationFile=log4j2.xml -cp ./build/libs/dag-placement.jar edu.vanderbilt.kharesp.dagPlacement.Vertex $graph_id $line 1>./log/err/$graph_id/"$vid".log 2>&1 ) & )
  done 
}< "$graph_description"
