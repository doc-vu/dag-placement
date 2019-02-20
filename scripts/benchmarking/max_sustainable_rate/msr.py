import os,sys,time
import pandas as pd
import numpy as np
from kazoo.client import KazooClient
sys.path.insert(0, './scripts/benchmarking/execute')
import guarded 


zk_connector='129.59.105.159:2181'
domain_id=0
log_dir='/home/shweta/workspace/research/dag-placement/log/msr'
processing_intervals=[1,5,10,15,20,25,30]
vertex_bbb='bbb-a702'
collector_bbb='bbb-ed97'

for proc in processing_intervals:
  for sel in [.5,1]:
    for rate in range(1,21):
      print('Running experiment for proc:%d, sel:%.1f and rate:%d\n'%(proc,sel,rate))

      #start zk
      zk=KazooClient(zk_connector)
      zk.start()
      #set runid
      if not zk.exists('/dom%d/runid'%(domain_id)):
        zk.ensure_path('/dom%d/runid'%(domain_id))
      zk.set('/dom%d/runid'%(domain_id),b'%d'%(rate))

      #make sub-dirs
      if not os.path.exists('%s/p%d/s%.1f/r%d'%(log_dir,proc,sel,rate)):
        os.makedirs('%s/p%d/s%.1f/r%d'%(log_dir,proc,sel,rate))
      if not os.path.exists('%s/p%d/s%.1f/r%d/graphs'%(log_dir,proc,sel,rate)):
        os.makedirs('%s/p%d/s%.1f/r%d/graphs'%(log_dir,proc,sel,rate))

      with open('%s/p%d/s%.1f/r%d/graphs/g1.txt'%(log_dir,proc,sel,rate),'w') as f:
        f.write('node;vertex;subscription;publication;selectivity;input_rate;sinks;sources;vertices;publication_rate,processing_interval\n')
        f.write('localhost;g1_v0;;g1_e01;%f;%f;1;1;3;%d;-1\n'%(1,1,rate))
        f.write('%s;g1_v1;g1_e01;g1_e12;%f;%f;1;1;3;%d;%d\n'%(vertex_bbb,sel,1,rate,proc))
        f.write('%s;g1_v2;g1_e12;;%f;%f;1;1;3;%d;-1\n'%(collector_bbb,1,sel,rate))

    
      start_ts=time.time()
      guarded.Guarded(zk_connector=zk_connector,\
        config_dir='%s/p%d/s%.1f/r%d'%(log_dir,proc,sel,rate),\
        remote_log_dir='/home/riaps/workspace/dag-placement/log/%d'%(domain_id),\
        local_log_dir='%s/p%d/s%.1f/r%d'%(log_dir,proc,sel,rate),\
        execution_time=120,domain_id=domain_id).run()
      end_ts=time.time()
      
      #stop zk
      zk.stop()
      print('Test configuration for proc:%d sel:%f rate:%d took %.1f min'%(proc,sel,rate,(end_ts-start_ts)/60.0))
