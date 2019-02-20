import sys,os,shutil
sys.path.insert(0, './scripts/benchmarking/execute')
import guarded


domain_id=3
zk_connector='129.59.105.159:2181'


for runid in range(2,6):
  for tid in range(1,10):
    config_dir='/home/shweta/workspace/research/dag-placement/log/linear_chain_overhead/structure/merge/run2/run%d/%d'%(runid,tid)
    log_dir='/home/shweta/workspace/research/dag-placement/log/linear_chain_overhead/structure/merge/run2/run%d/%d'%(runid,tid)
    
    #ensure that configuration directory exists
    if not os.path.exists(config_dir):
      os.makedirs(config_dir)
    if not os.path.exists('%s/graphs'%(config_dir)):
      os.makedirs('%s/graphs'%(config_dir))
    #copy graph configurations from run1 to current run
    for file_name in os.listdir('/home/shweta/workspace/research/dag-placement/log/linear_chain_overhead/structure/merge/run2/run1/%d/graphs'%(tid)):
      shutil.copy('/home/shweta/workspace/research/dag-placement/log/linear_chain_overhead/structure/merge/run2/run1/%d/graphs/%s'%(tid,file_name),
        '/home/shweta/workspace/research/dag-placement/log/linear_chain_overhead/structure/merge/run2/run%d/%d/graphs/%s'%(runid,tid,file_name))
    
    #execute experiment
    guarded.Guarded(zk_connector=zk_connector,\
      config_dir=config_dir,\
      remote_log_dir='/home/riaps/workspace/dag-placement/log/%d'%(domain_id),\
      local_log_dir=log_dir,\
      execution_time=120,domain_id=domain_id).run()
  
for runid in range(2,6):
  for tid in range(1,10):
    for config in range(1,3):
      config_dir='/home/shweta/workspace/research/dag-placement/log/linear_chain_overhead/structure/merge/run2/run%d/red2/%d_%d'%(runid,tid,config)
      log_dir='/home/shweta/workspace/research/dag-placement/log/linear_chain_overhead/structure/merge/run2/run%d/red2/%d_%d'%(runid,tid,config)

      #ensure that configuration directory exists
      if not os.path.exists(config_dir):
        os.makedirs(config_dir)
      if not os.path.exists('%s/graphs'%(config_dir)):
        os.makedirs('%s/graphs'%(config_dir))

      #copy graph configurations from run1 to current run
      for file_name in os.listdir('/home/shweta/workspace/research/dag-placement/log/linear_chain_overhead/structure/merge/run2/run1/red2/%d_%d/graphs'%(tid,config)):
        shutil.copy('/home/shweta/workspace/research/dag-placement/log/linear_chain_overhead/structure/merge/run2/run1/red2/%d_%d/graphs/%s'%(tid,config,file_name),
          '/home/shweta/workspace/research/dag-placement/log/linear_chain_overhead/structure/merge/run2/run%d/red2/%d_%d/graphs/%s'%(runid,tid,config,file_name))
      
      #execute experiment
      guarded.Guarded(zk_connector=zk_connector,\
        config_dir=config_dir,\
        remote_log_dir='/home/riaps/workspace/dag-placement/log/%d'%(domain_id),\
        local_log_dir=log_dir,\
        execution_time=120,domain_id=domain_id).run()

for runid in range(2,6):
  for tid in range(1,10):
    for config in range(1,2):
      config_dir='/home/shweta/workspace/research/dag-placement/log/linear_chain_overhead/structure/merge/run2/run%d/red1/%d_%d'%(runid,tid,config)
      log_dir='/home/shweta/workspace/research/dag-placement/log/linear_chain_overhead/structure/merge/run2/run%d/red1/%d_%d'%(runid,tid,config)

      #ensure that configuration directory exists
      if not os.path.exists(config_dir):
        os.makedirs(config_dir)
      if not os.path.exists('%s/graphs'%(config_dir)):
        os.makedirs('%s/graphs'%(config_dir))

      #copy graph configurations from run1 to current run
      for file_name in os.listdir('/home/shweta/workspace/research/dag-placement/log/linear_chain_overhead/structure/merge/run2/run1/red1/%d_%d/graphs'%(tid,config)):
        shutil.copy('/home/shweta/workspace/research/dag-placement/log/linear_chain_overhead/structure/merge/run2/run1/red1/%d_%d/graphs/%s'%(tid,config,file_name),
          '/home/shweta/workspace/research/dag-placement/log/linear_chain_overhead/structure/merge/run2/run%d/red1/%d_%d/graphs/%s'%(runid,tid,config,file_name))
      
      #execute experiment
      guarded.Guarded(zk_connector=zk_connector,\
        config_dir=config_dir,\
        remote_log_dir='/home/riaps/workspace/dag-placement/log/%d'%(domain_id),\
        local_log_dir=log_dir,\
        execution_time=120,domain_id=domain_id).run()
