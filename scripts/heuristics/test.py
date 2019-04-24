import sys,os,shutil
sys.path.insert(0, './scripts/benchmarking/execute')
import guarded
import numpy as np


domain_id=1
zk_connector='129.59.105.159:2181'
config_dir='/home/shweta/workspace/research/dag-placement/log/test_placement/1'

#execute experiment
guarded.Guarded(zk_connector=zk_connector,\
  config_dir=config_dir,\
  remote_log_dir='/home/riaps/workspace/dag-placement/log/%d'%(domain_id),\
  local_log_dir=config_dir,\
  execution_time=120,domain_id=domain_id).run()
