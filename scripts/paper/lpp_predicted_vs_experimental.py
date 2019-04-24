import os
import numpy as np
import makespan

network_cost=10
log_dir='/home/shweta/workspace/research/dag-placement/log/greedy_pmax_20_rmax_15_nc_10'

nodes_used=[]
observed_latencies_to_plot=[]
predicted_latencies_to_plot=[]
error=[]
config=1

vertex_gid={ 6: [1,5,2],
  7: [1,3,4],
  8: [3,4,5],
}

for v in range(6,9):
  for gid in vertex_gid[v]:
    experimentally_observed_makespan,predicted_makespan,node_count=\
      makespan.get_makespan_prediction_enodes(v,'%s/v%d/g%d/%d/lpp'%(log_dir,v,gid,config),network_cost)
  
    observed_latencies_to_plot.append(experimentally_observed_makespan)
    predicted_latencies_to_plot.append(predicted_makespan)
    error.append(abs(experimentally_observed_makespan-predicted_makespan))
    nodes_used.append(node_count)

print('observed:\n%s'%(observed_latencies_to_plot))
print('predicted:\n%s'%(predicted_latencies_to_plot))
print('error:\n%s'%(error))
print('nodes:\n%s'%(nodes_used))
