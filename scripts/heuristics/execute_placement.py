import sys,os,shutil
sys.path.insert(0, './scripts/benchmarking/execute')
import guarded
import numpy as np


domain_id=1
zk_connector='129.59.105.159:2181'

#log_dir='/home/shweta/workspace/research/dag-placement/log/linear_chain_overhead/structure/split/run2'
#for tid in range(1,4):
#  print('\n')
#  number_of_graphs=len(os.listdir('%s/run2/red/%d_1/summary'%(log_dir,tid)))-1
#  for gid in range(number_of_graphs):
#    l=[]
#    for runid in range(1,6):
#      with open('%s/run%d/red/%d_1/summary/summary_g%d.csv'%(log_dir,runid,tid,gid+1),'r') as f:
#        next(f)
#        for line in f:
#          sink,avg,l90th= line.strip().split(',')
#          l.append(float(l90th))
#    #print(l)
#    print('tid:%d g%d mean:%.2f std:%.2f'%(tid,gid+1,np.mean(l),np.std(l)))
        
#for tid in [3]:
#  config_dir='/home/shweta/workspace/research/dag-placement/log/linear_chain_overhead/structure/composition/equivalent_structure/%d'%(tid)
#  #execute experiment
#  guarded.Guarded(zk_connector=zk_connector,\
#    config_dir=config_dir,\
#    remote_log_dir='/home/riaps/workspace/dag-placement/log/%d'%(domain_id),\
#    local_log_dir=config_dir,\
#    execution_time=120,domain_id=domain_id).run()

#for tid in range(1,6):
#  config_dir='/home/shweta/workspace/research/dag-placement/log/linear_chain_overhead/structure/composition/equivalent_structure2/%d'%(tid)
#  #execute experiment
#  guarded.Guarded(zk_connector=zk_connector,\
#    config_dir=config_dir,\
#    remote_log_dir='/home/riaps/workspace/dag-placement/log/%d'%(domain_id),\
#    local_log_dir=config_dir,\
#    execution_time=120,domain_id=domain_id).run()
#
for tid in range(5,6):
  config_dir='/home/shweta/workspace/research/dag-placement/log/linear_chain_overhead/structure/composition/sub_structure6/%d'%(tid)
  #execute experiment
  guarded.Guarded(zk_connector=zk_connector,\
    config_dir=config_dir,\
    remote_log_dir='/home/riaps/workspace/dag-placement/log/%d'%(domain_id),\
    local_log_dir=config_dir,\
    execution_time=120,domain_id=domain_id).run()

#for tid in range(1,6):
#  config_dir='/home/shweta/workspace/research/dag-placement/log/linear_chain_overhead/structure/composition/sub_structure2/%d'%(tid)
#  #execute experiment
#  guarded.Guarded(zk_connector=zk_connector,\
#    config_dir=config_dir,\
#    remote_log_dir='/home/riaps/workspace/dag-placement/log/%d'%(domain_id),\
#    local_log_dir=config_dir,\
#    execution_time=120,domain_id=domain_id).run()

#for runid in range(1,2):
#  for tid in range(1,4):
#    config_dir='/home/shweta/workspace/research/dag-placement/log/linear_chain_overhead/structure/split/run2/run%d/%d'%(runid,tid)
#    log_dir='/home/shweta/workspace/research/dag-placement/log/linear_chain_overhead/structure/split/run2/run%d/%d'%(runid,tid)
#    
#    #execute experiment
#    guarded.Guarded(zk_connector=zk_connector,\
#      config_dir=config_dir,\
#      remote_log_dir='/home/riaps/workspace/dag-placement/log/%d'%(domain_id),\
#      local_log_dir=log_dir,\
#      execution_time=120,domain_id=domain_id).run()
#  
#for runid in range(1,2):
#  for tid in range(1,4):
#    for config in range(1,3):
#      config_dir='/home/shweta/workspace/research/dag-placement/log/linear_chain_overhead/structure/split/run2/run%d/red/%d_%d'%(runid,tid,config)
#      log_dir='/home/shweta/workspace/research/dag-placement/log/linear_chain_overhead/structure/split/run2/run%d/red/%d_%d'%(runid,tid,config)
#
#      #execute experiment
#      guarded.Guarded(zk_connector=zk_connector,\
#        config_dir=config_dir,\
#        remote_log_dir='/home/riaps/workspace/dag-placement/log/%d'%(domain_id),\
#        local_log_dir=log_dir,\
#        execution_time=120,domain_id=domain_id).run()
