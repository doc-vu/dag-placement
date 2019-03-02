import sys
sys.path.insert(0, './scripts/parse_dag')
import dag_utils
import numpy as np

class Greedy(object):
  def replicas(self,adj):
    vertices=np.shape(adj)[0]
    rep={}
    for x in range(vertices):  
      patterns=set()
      for path in [p for p in dag_utils.find_all_paths(adj) if (x in p)]:
        idx=path.index(x)
        partial_path=','.join([str(path[i]) for i in range(idx)])
        patterns.add(partial_path)
      rep[x]=len(patterns)
    return rep

  def get_hosting_node(self,x,placement):
    for node,vertices in placement.itmes():
      if x in vertices:
        return node
    return None

  def get_partial_paths_per_node(self,paths,vertex_node):
    node_pp={n:[] for n in set(vertex_node.values())}
    for path in paths:
      prev_node=vertex_node[path[0]]
      pp=[]
      for v in path:
        curr_node=vertex_node[v]
        if curr_node==prev_node:
          pp.append(v)
        else:
          node_pp[prev_node].append(pp)
          pp=[v]
          prev_node=curr_node
      node_pp[prev_node].append(pp)
    return node_pp
 
  def predict(self,foreground_chain,interference_chains,params):
    sum_of_processing_intervals=0
    for linear_chain in interference_chains:
      sum_of_processing_intervals+=sum([params['proc'][x] for x in linear_chain])

    sum_of_processing_intervals+=sum([params['proc'][x] for x in foreground_chain])
    return  sum_of_processing_intervals*len(foreground_chain)
 
  def get_interference_chains_for_paths(self,adj,paths):
    interference={}
    for curr_path in paths:
      track_rep=self.replicas(adj)
      curr_path_s=','.join([str(x) for x in curr_path])
      interference[curr_path_s]=[]
      for x in curr_path:
        track_rep[x]-=1
      for path in paths:
        path_s=','.join([str(x) for x in path])
        if path_s==curr_path_s:
          continue
        ipath=[]
        for x in path:
          if (track_rep[x]>0):
             ipath.append(x)
             track_rep[x]-=1
        interference[curr_path_s].append(ipath)
    return interference

  def compute_path_latency(self,curr_path,interference,vertex_node,network):
    interference_paths=interference[','.join([str(i) for i in curr_path])]
    node_pp=self.get_partial_paths_per_node(interference_paths,vertex_node)

    curr_path_latency=0
    prev_node=vertex_node[curr_path[0]]
    pp=[]
    for x in curr_path:
      curr_node=vertex_node[x]
      if curr_node==prev_node:
        pp.append(x)
      else:
        curr_path_latency+=(self.predict(pp,node_pp[prev_node],params)+network)
        prev_node=curr_node
        pp=[x]
    curr_path_latency+=self.predict(pp,node_pp[prev_node],params)
    return curr_path_latency
            
  def place(self,adj,params,network):
    sources=list(np.where(~adj.any(axis=0))[0])
    sinks=list(np.where(~adj.any(axis=1))[0])
    bfs_order=self.bfs(adj,sources[0])
    print('Order in which vertices will be placed:%s'%(bfs_order))
    
    placement={}

    for idx,v in enumerate(bfs_order):
      if (v in sources) or (v in sinks):
        continue
      #get sub-graph
      sub_graph_vertices=sorted([bfs_order[x] for x in range(idx+1)])

      sub_graph=adj[np.ix_(sub_graph_vertices,sub_graph_vertices)]
      paths=dag_utils.find_all_paths(sub_graph)
      #get all paths in sub-graph
      corrected_paths=[[sub_graph_vertices[x] for x in p ]for p in paths]
      #drop source and sink vertices from corrected paths
      corrected_paths=[[x for x in p if ((x not in sources) and (x not in sinks))] for p in corrected_paths]

      #create interference paths for each path 
      interference=self.get_interference_chains_for_paths(adj,corrected_paths)
      
      #compute each path's latency when current vertex v is placed on each eligible physical node
      node_makespan={}
      for node in list(placement.keys())+['bbb%d'%(len(placement)+1)]:
        vertex_node={}
        vertex_node[v]=node
        for host,vertices in placement.items(): 
          for vertex in vertices:
            vertex_node[vertex]=host

        configuration_latencies=[]
        for curr_path in corrected_paths:
          curr_path_latency=self.compute_path_latency(curr_path,interference,vertex_node,network)
          configuration_latencies.append(curr_path_latency)
        node_makespan[node]=max(configuration_latencies)
      #select node which gives minimum makespan of sub-dag 
      selected_node=min(node_makespan.items(),key=lambda x: x[1])[0]
      
      if selected_node not in placement:
        placement[selected_node]=[v]
      else:
        placement[selected_node].append(v)
    return placement

  def get_latencies_of_all_paths_after_placement(self,adj,placement,params,network):
    #get source and sink vertices
    sources=list(np.where(~adj.any(axis=0))[0])
    sinks=list(np.where(~adj.any(axis=1))[0])
    #find all paths in DAG
    paths=dag_utils.find_all_paths(adj)
    corrected_paths=[[x for x in p if ((x not in sources) and (x not in sinks))] for p in paths]
    #create interference paths for each path 
    interference=self.get_interference_chains_for_paths(adj,corrected_paths)

    vertex_node={}
    for host,vertices in placement.items(): 
      for vertex in vertices:
        vertex_node[vertex]=host

    for curr_path in corrected_paths:
      curr_path_latency=self.compute_path_latency(curr_path,interference,vertex_node,network)
      print('latency for path:%s is %d'%(curr_path,curr_path_latency+2*network))

  def bfs(self,adj,s):
    vertices=np.shape(adj)[0]
    bfs_order=[]
    queue=[]
    visited={x:False for x in range(vertices)}
    visited[s]=True
    queue.append(s)
    while len(queue)>0:
      v=queue.pop(0)
      bfs_order.append(v)
      for next_v in np.nonzero(adj[v,:])[0]:
        if visited[next_v]==False:
          queue.append(next_v)
          visited[next_v]=True
    return bfs_order 

if __name__=="__main__":
  example=[[0,1,0,0,1,0],
    [0,0,1,1,0,0],
    [0,0,0,0,1,0],
    [0,0,0,0,0,1],
    [0,0,0,0,0,1],
    [0,0,0,0,0,0]]
  params={'r':10,'proc':{0:0,5:0,1:5,2:5,3:5,4:5}}
  placement=Greedy().place(np.array(example),params,20)
  Greedy().get_latencies_of_all_paths_after_placement(np.array(example),placement,params,20)
