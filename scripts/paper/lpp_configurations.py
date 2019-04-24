import os
import numpy as np
import makespan

vs=[6,7,8]
gid=1
network_cost=20
log_dir='/home/shweta/workspace/research/dag-placement/log/greedy_pmax_20_rmax_15_nc_10'

nodes_used=[]
observed_latencies_to_plot=[]
predicted_latencies_to_plot=[]

for v in vs:
  for config in range(1,4):
    if v==8:
      gid=3
    observed,predicted,nodes=makespan.get_makespan_prediction_enodes(v,'%s/v%d/g%d/%d/lpp'%(log_dir,v,gid,config),network_cost)
    observed_latencies_to_plot.append(observed)
    predicted_latencies_to_plot.append(predicted)
    nodes_used.append(nodes)

print('observed:\n%s'%(observed_latencies_to_plot))
print('predicted:\n%s'%(predicted_latencies_to_plot))
print('nodes:\n%s'%(nodes_used))
