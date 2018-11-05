import numpy as np
import pandas as pd
import os

def dag_latency(path):
  output_streams=os.listdir(path)
  edge_latencies={}
  for s in output_streams:
    data=pd.read_csv('%s/%s'%(path,s),\
        names=['edge_id','sample_id','latency'],
        skiprows=1,delimiter=',')
    edge_latencies[s]=np.mean(data['latency'][10:])
  return np.mean(list(edge_latencies.values()))

def get_latencies_for_all_dags(log_dir,output_file):
  with open(output_file,'w') as wf:
    wf.write('v,g1,g2,g3,g4,g5,mean,std\n')
    for v in range(4,7):
      graph_latencies=[]
      for g in range(1,6):
        graph_latencies.append(dag_latency('%s/v%d/g%d/v_%d_g_%d_c1/dag'%(log_dir,v,g,v,g)))
      wf.write('%d,%f,%f,%f,%f,%f,%f,%f\n'%(v,graph_latencies[0],\
        graph_latencies[1],graph_latencies[2],\
        graph_latencies[3],graph_latencies[4],\
        np.mean(graph_latencies),np.std(graph_latencies)))

get_latencies_for_all_dags('./log/single/erdos_renyi/rate10/','./data/single/rate10/single.csv')
