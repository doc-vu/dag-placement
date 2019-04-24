import numpy as np

def get_makespan_prediction_enodes(v,log_dir,network_cost):
  observed_path_latency={}
  with open('%s/summary/summary_g1.csv'%(log_dir),'r') as f:
    next(f) #skip header
    for line in f:
      edge,avg,l_90th= line.rstrip().split(',')
      g,eid=edge.split('_')
      observed_path_latency[eid[1:]]=float(l_90th)

  selected_edge=max(observed_path_latency,key=observed_path_latency.get)


  predicted_path_latency={}
  with open('%s/heuristic/prediction.csv'%(log_dir),'r') as f:
    next(f)
    for line in f:
      path,sep,l_90th=line.strip().rpartition(',')
      path=path.replace(',','')+ '%d'%(v+1)
      if selected_edge in path:
        predicted_path_latency[path]=float(l_90th)

 
  node_count=0 
  placement={}
  with open('%s/heuristic/placement.csv'%(log_dir),'r') as f:
    next(f)
    for line in f:
      node_count+=1
      node,vertices=line.strip().split(';')
      for x in vertices.split(','):
        placement[int(x)]=node
  
  nw_cost_per_path={}
  for path,latency in predicted_path_latency.items():
    cost=2*network_cost
    path_vertices=[int(x) for x in path]
    prev_node=placement[path_vertices[0]]
    for i in range(1,len(path_vertices)-1):
      curr_node=placement[path_vertices[i]]
      if curr_node!=prev_node:
        cost+=network_cost
        prev_node=curr_node
    nw_cost_per_path[path]=cost
  
  predicted_makespan=np.mean([nw_cost_per_path[path]+predicted_path_latency[path] for path in predicted_path_latency.keys()])
  experimentally_observed_makespan=np.mean([nw_cost_per_path[path]+observed_path_latency[selected_edge] for path in predicted_path_latency.keys()])

  return (experimentally_observed_makespan,predicted_makespan,node_count)

if __name__=="__main__":
  print(get_makespan_prediction_enodes(8,'log/greedy/v8/g5/1/lpp',20))
  print(get_makespan_prediction_enodes(8,'log/greedy/v8/g5/1/sum',20))
  print(get_makespan_prediction_enodes(8,'log/greedy/v8/g5/1/const',20))
