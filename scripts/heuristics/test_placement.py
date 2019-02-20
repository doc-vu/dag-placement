import sys,random,os
import numpy as np
sys.path.insert(0, './scripts/parse_dag')
import dag_utils
from sklearn.externals import joblib
from sklearn.neural_network import MLPRegressor

def get_vertex_features(gid,adj):
  selectivities=[.5,1]
  processing_intervals=[1,5,10,15,20,25,30]

  v=adj.shape[0]
  source_vertices=dag_utils.get_source_vertices(adj).tolist()
  sink_vertices=dag_utils.get_sink_vertices(adj).tolist()
 
  while True:
    rate=random.randint(1,20)
    #randomly generate selectivites for each vertex
    sel_vec=[1 if i in source_vertices+sink_vertices \
      else selectivities[random.randint(0,len(selectivities)-1)] for i in range(v)]

    #compute incoming rates 
    input_rates=dag_utils.find_incoming_rate(adj,sel_vec,1) 
    
    #ensure that 1<=rate<=20 for all vertices in the DAG
    if all((int(rate*int(x))>=1 and int(rate*int(x))<=20) for x in input_rates):
      break

  #randomly generate processing intervals for each vertex
  proc_vec=[-1 if i in source_vertices+sink_vertices \
    else processing_intervals[random.randint(0,len(processing_intervals)-1)] for i in range(v)] 
 
  #get subscription information
  subscription={vid:','.join(['%s_e%d%d'%(gid,x,vid)  for x in np.nonzero(adj[:,vid])[0]]) for vid in range(v)}

  #get publication information
  publication={vid: ','.join(['%s_e%d%d'%(gid,vid,x)  for x in np.nonzero(adj[vid,:])[0]]) for vid in range(v)}

  features={vid:'%s_v%d;%s;%s;%f;%f;%d;%d;%d;%d;%d'%(gid,vid,\
    subscription[vid],publication[vid],\
    sel_vec[vid],input_rates[vid],\
    len(sink_vertices),len(source_vertices),v,\
    rate,proc_vec[vid]) for vid in range(v)}

  return features

def get_input_feature_vector(vertices,features):
  feature_vectors={}
  for currv in vertices:
    vid,subscription,publication,\
    sel,inp_rate,no_sinks,no_sources,v_count,\
    rate,proc=features[currv].split(';')
    fp=int(proc)
    fr=int(float(inp_rate)*int(rate))
    fs=float(sel)
    bsri=0
    bsp=0
    bsl=0
    for v in vertices:
      if v==currv:
        continue
      vid,subscription,publication,\
      sel,inp_rate,no_sinks,no_sources,v_count,\
      rate,proc=features[v].split(';')
      bsri+=int(float(inp_rate)*int(rate))
      bsp+=int(proc)
      bsl+=(int(proc)*int(float(inp_rate)*int(rate)))/1000.0
    X=[[fp,fr,fs,bsri,bsp,bsl]]
    feature_vectors[currv]=X
  return feature_vectors

def check_feasibility_of_placement(vertices,features):
  if(len(vertices)==1):
    return True
  else:
    scaler=joblib.load('models/k%d/classification/scaler.pkl'%(len(vertices)))
    model=joblib.load('models/k%d/classification/model.pkl'%(len(vertices)))
    feature_vectors=get_input_feature_vector(vertices,features)
    for v,vec in feature_vectors.items():
      if model.predict(scaler.transform(vec))==1:
        return False
    return True

def get_predicted_latency(vertices,features):
  if len(vertices)==1:
    vid,subscription,publication,\
    sel,inp_rate,no_sinks,no_sources,v_count,\
    rate,proc=features[vertices[0]].split(';')
    return {vertices[0]:int(proc)+5}
  else:
    scaler=joblib.load('models/k%d/regression/scaler.pkl'%(len(vertices)))
    model=joblib.load('models/k%d/regression/model.pkl'%(len(vertices)) )
    feature_vectors=get_input_feature_vector(vertices,features)
    latency_predictions={}
    for v,vec in feature_vectors.items():
      latency_predictions[v]= np.exp(model.predict(scaler.transform(vec))[0])
    return latency_predictions

def get_hosting_node(placement,v):
  for n,vset in placement.items():
    if v in vset:
      return n
  return None

def greedy_placement(adj,features):
  K=5
  N=10
  paths=dag_utils.find_all_paths(adj)
  sources=dag_utils.get_source_vertices(adj).tolist()
  sinks=dag_utils.get_sink_vertices(adj).tolist()

  #sources and sinks are already placed 
  placed_vertices=set(sources+sinks)
  node_vertex_placement={'localhost':sources+sinks}

  for path in paths:
    for idx,vertex in enumerate(path):

      if vertex in placed_vertices:
        continue
      else:
        #compute latency for placement on existing nodes
        node_prediction={}
        for node,vertices in node_vertex_placement.items():
          #intermediate vertices can't be placed with source/sink
          if node=='localhost':
            continue
          #if node already has K vertices, then skip this node
          if len(vertices)==K:
            continue
  
          #check if placement on this node is feasible 
          if check_feasibility_of_placement(vertices+[vertex],features):
            prediction=get_predicted_latency(vertices+[vertex],features)[vertex]
            if idx==0:
              node_prediction[node]=prediction
            else:
              preceeding_vertex_node=get_hosting_node(node_vertex_placement,path[idx-1])
              if (preceeding_vertex_node==node):
                node_prediction[node]=prediction
              else:
                node_prediction[node]=prediction+ N

        #compute latency for placement on a new node
        if idx==0:
          node_prediction['bbb%d'%(len(node_vertex_placement))]=get_predicted_latency([vertex],features)[vertex]
        else:
          node_prediction['bbb%d'%(len(node_vertex_placement))]=get_predicted_latency([vertex],features)[vertex]+N

        #find node with minimal latency of placement
        selected_node=min(node_prediction,key=node_prediction.get)

        #place vertex on selected_node
        if selected_node in node_vertex_placement:
          node_vertex_placement[selected_node].append(vertex)
        else:
          node_vertex_placement[selected_node]=[vertex]

        placed_vertices.add(vertex)
  return node_vertex_placement

def get_predicted_path_latencies(adj,features,placement_map):
  N=0
  paths=dag_utils.find_all_paths(adj)
  source_vertices=dag_utils.get_source_vertices(adj).tolist()
  sink_vertices=dag_utils.get_sink_vertices(adj).tolist()
  res={}
  for path in paths:
    path_latency=0
    for idx,vertex in enumerate(path):
      node=get_hosting_node(placement_map,vertex)
      if (vertex in source_vertices) or (vertex in sink_vertices):
        clatency=0
      else:
        clatency=get_predicted_latency(placement_map[node],features)[vertex]

      if idx==0:
        path_latency+=clatency
      else:
        prev_node=get_hosting_node(placement_map,path[idx-1])
        if prev_node==node:
          path_latency+=clatency
        else:
          path_latency+=clatency+N
    res[','.join([str(x) for x in path])]=path_latency
  return res

def get_placed_vertex_latency(adj,features,placement_map):
  v=len(features)
  source_vertices=dag_utils.get_source_vertices(adj).tolist()
  sink_vertices=dag_utils.get_sink_vertices(adj).tolist()
  res={}
  for vid in range(v):
    if vid in source_vertices+sink_vertices:
      res[vid]=0
    else:
      node=get_hosting_node(placement_map,vid)
      clatency=get_predicted_latency(placement_map[node],features)[vid]
      res[vid]=clatency
  return res
  
 
bbb_nodes={
  'bbb1':'bbb-a702',
  'bbb2':'bbb-ed97',
  'bbb3':'bbb-bce4',
  'bbb4':'bbb-bcf6',
  'localhost': 'localhost',
}

for tid in range(21,41):
  gid='g4'
  dag='dags/fan_in_fan_out2/v3/adj/v3_g4.txt'
  adj=np.loadtxt(dag,dtype=int,delimiter=',') 
  features=get_vertex_features(gid,adj)
  placement=greedy_placement(adj,features)
 
  if not os.path.exists('/home/shweta/workspace/research/dag-placement/log/heuristics/greedy/v3/g4/%d/graphs'%(tid)):
    os.makedirs('/home/shweta/workspace/research/dag-placement/log/heuristics/greedy/v3/g4/%d/graphs'%(tid))
  if not os.path.exists('/home/shweta/workspace/research/dag-placement/log/heuristics/greedy/v3/g4/%d/predictions'%(tid)):
    os.makedirs('/home/shweta/workspace/research/dag-placement/log/heuristics/greedy/v3/g4/%d/predictions'%(tid))

  with open('/home/shweta/workspace/research/dag-placement/log/heuristics/greedy/v3/g4/%d/graphs/g4.txt'%(tid),'w') as f:
    f.write('node;vertex;subscription;publication;selectivity;input_rate;sinks;sources;vertices;publication_rate,processing_interval\n')
    for node,vlist in placement.items():
      for v in vlist:
        f.write('%s;%s\n'%(bbb_nodes[node],features[v]))
  
  with open('/home/shweta/workspace/research/dag-placement/log/heuristics/greedy/v3/g4/%d/predictions/paths.txt'%(tid),'w') as f:
    for path,l in get_predicted_path_latencies(adj,features,placement).items():
      f.write('%s;%f\n'%(path,l))
  
  with open('/home/shweta/workspace/research/dag-placement/log/heuristics/greedy/v3/g4/%d/predictions/vertices.txt'%(tid),'w') as f:
    for vid,l in get_placed_vertex_latency(adj,features,placement).items():
      f.write('%d;%f\n'%(vid,l))
