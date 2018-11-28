import os,sys
sys.path.insert(0, './scripts/benchmarking/execute')
import experiment
import pandas as pd
import numpy as np

processing_interval=10
local_log_dir='/home/shweta/workspace/research/dag-placement/log/max_sustainable_rate/p%d'%(processing_interval)
publisher_node='localhost'
subscriber_node='bbb-96af'

for rate in range(51,101):
  #create configuration file
  if not os.path.exists('%s/r%d/graphs'%(local_log_dir,rate)):
    os.makedirs('%s/r%d/graphs'%(local_log_dir,rate))  
  with open('%s/r%d/graphs/g1.txt'%(local_log_dir,rate),'w') as f:
    f.write('node;vertex;subscription;publication;selectivity;input_rate;sinks;sources;vertices;publication_rate,processing_interval\n')
    for i in range(rate):
      f.write('%s;g1_v%d;;g1_e%d0;%f;%f;1;%d;%d;1;-1\n'%(publisher_node,i+1,i+1,1,1,rate,rate+1))
    f.write('%s;g1_v0;%s;;%f;%f;1;%d;%d;1;%d\n'%(subscriber_node,','.join(['g1_e%d0'%(v+1) for v in range(rate)]),1,rate,rate,rate+1,processing_interval))

  #run experiment
  experiment.Experiment(zk_connector='129.59.105.159:2181',\
    config_dir='%s/r%d'%(local_log_dir,rate),
    remote_log_dir='/home/riaps/workspace/dag-placement/log',
    local_log_dir='%s/r%d'%(local_log_dir,rate),
    execution_time=120,
    zmq=False).run()

  #check if latency is within 1 second
  data=pd.read_csv('%s/r%d/summary/summary_g1.csv'%(local_log_dir,rate),
    names=['edge','avg','90th'],skiprows=1,delimiter=',')
  latency_90th=np.mean(data['90th'])

  print('\n\n\n\n\nrate:%d latency(90th percentile):%f'%(rate,latency_90th))

  if (latency_90th>5000):
    break
