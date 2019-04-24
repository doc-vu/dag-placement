import os
import makespan


vcounts=[6]
gids={
  6: [1,5,2],
  8: [3,4,5],
}

network_cost=10
log_dir='/home/shweta/workspace/research/dag-placement/log/greedy_paper'

nodes_used={}
observed={}
predicted={}
error={}

for method in ['lpp','sum','const','naive-1']:
  observations=[]
  predictions=[]
  nodes=[]
  err=[]
  for vcount in vcounts:
    for gid in gids[vcount]:
      observation,prediction,enodes= makespan.get_makespan_prediction_enodes(vcount,'%s/v%d/g%d/1/%s/'%(log_dir,vcount,gid,method),network_cost)
      observations.append(observation)
      predictions.append(prediction)
      nodes.append(enodes)
      err.append(abs(observation-prediction))

  observed[method]=observations
  predicted[method]=predictions
  nodes_used[method]=nodes
  error[method]=err
  print(observed)
  


print('enodes:\n%s\n'%(nodes_used))
print('makespan:\n%s\n'%(observed))
print('prediction:\n%s\n'%(predicted))
print('error:\n%s\n'%(error))

