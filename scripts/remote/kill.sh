#!/bin/bash
if [ $# -ne 1 ]; then
  echo 'usage:' $0 'domain_id' 
  exit 1
fi

domain_id=$1

ps -ef | grep -v grep | grep -v kill.yml | grep -w dagPlacement.dds | while read -r op; 
do
  if echo $op | grep -q "[$domain_id]$"; then 
    pid=`echo $op | awk '{print $2}'`
    kill -9  $pid
  fi
done
