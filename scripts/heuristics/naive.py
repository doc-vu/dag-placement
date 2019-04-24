import sys,random,os
sys.path.insert(0, './scripts/parse_dag')
from sklearn.externals import joblib
import dag_utils
import numpy as np

class Naive(object):
  def get_hosting_node(self,x,placement):
    for node,vertices in placement.itmes():
      if x in vertices:
        return node
    return None

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


  def predict(self,method,foreground_chain,interference_chains,params):
    return self.predict3(foreground_chain,interference_chains,params)


  def predict2(self,foreground_chain,interference_chains,params):
    sum_of_processing_intervals=0
    sum_of_processing_intervals+=sum([params['proc'][x] for x in foreground_chain])
    for linear_chain in interference_chains:
      sum_of_processing_intervals+=sum([params['proc'][x] for x in linear_chain if x not in foreground_chain])
    return  sum_of_processing_intervals*len(foreground_chain)

  def predict3(self,foreground_chain,interference_chains,params):
    sum_of_processing_intervals=0
    sum_of_processing_intervals+=sum([params['proc'][x] for x in foreground_chain])
    return sum_of_processing_intervals
 
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


  def compute_path_latency(self,method,curr_path,interference,vertex_node,params,network):
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
        curr_path_latency+=(self.predict(method,pp,node_pp[prev_node],params)+network)
        prev_node=curr_node
        pp=[x]
    curr_path_latency+=self.predict(method,pp,node_pp[prev_node],params)
    return curr_path_latency
            
  def place(self,method,adj,params,network):
    v_per_node=int(method.split('-')[1])
    number_of_vertices=np.shape(adj)[0]
    sources=list(np.where(~adj.any(axis=0))[0])
    sinks=list(np.where(~adj.any(axis=1))[0])
    bfs_order=self.bfs(adj,sources[0])
    
    placement={}

    counter=0
    for idx,v in enumerate(bfs_order):
      if (v in sources) or (v in sinks):
        continue
      if len(placement)==0:
        placement['bbb%d'%(len(placement)+1)]=[v]
        counter+=1
      elif counter==v_per_node:
        placement['bbb%d'%(len(placement)+1)]=[v]
        counter=1
      else:
        placement['bbb%d'%(len(placement))].append(v)
        counter+=1

    return placement

  def get_latencies_of_all_paths_after_placement(self,method,adj,placement,params,network):
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

    path_latency={}
    for curr_path in corrected_paths:
      curr_path_latency=self.compute_path_latency(method,curr_path,interference,vertex_node,params,network)
      path_latency[','.join([str(i) for i in curr_path])]=curr_path_latency+2*network

    return path_latency

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
  network_cost=20
  ineligible_dags=['v5_g3.txt','v7_g5.txt','v8_g1.txt','v8_g2.txt']
  dag_dir='/home/shweta/workspace/research/dag-placement/dags/fan_in_fan_out2'
  log_dir='/home/shweta/workspace/research/dag-placement/log/greedy_paper'
  for v in range(6,9):
    graphs=os.listdir('%s/v%d/adj'%(dag_dir,v))
    for gid in range(1,len(graphs)+1):
      if 'v%d_g%d.txt'%(v,gid) in ineligible_dags:
        continue
      for tid in range(1,2):
        adj=np.loadtxt('%s/v%d/adj/v%d_g%d.txt'%(dag_dir,v,v,gid),dtype=int,delimiter=',')
        #load parameters
        with open('%s/v%d/g%d/%d/sum/heuristic/params.csv'%(log_dir,v,gid,tid),'r') as f:
          next(f)#skip header
          rate,p=next(f).strip().split(';')
        
        processing_intervals=p.split(',')
        params={}
        params['rate']=int(rate)
        params['proc']={x:int(processing_intervals[x-1]) for x in range(1,len(processing_intervals)+1)}
        
        for method in ['naive-1','naive-2']:
          print('Placing for v:%d gid:%d tid:%d method:%s'%(v,gid,tid,method))
          placement=Naive().place(method,adj,params,network_cost)
          path_latencies=Naive().get_latencies_of_all_paths_after_placement(method,adj,placement,params,0)
          
          if not os.path.exists('%s/v%d/g%d/%d/%s'%(log_dir,v,gid,tid,method)):
            os.makedirs('%s/v%d/g%d/%d/%s'%(log_dir,v,gid,tid,method))
          if not os.path.exists('%s/v%d/g%d/%d/%s/graphs'%(log_dir,v,gid,tid,method)):
            os.makedirs('%s/v%d/g%d/%d/%s/graphs'%(log_dir,v,gid,tid,method))
          if not os.path.exists('%s/v%d/g%d/%d/%s/heuristic'%(log_dir,v,gid,tid,method)):
            os.makedirs('%s/v%d/g%d/%d/%s/heuristic'%(log_dir,v,gid,tid,method))
          #write parameters
          with open('%s/v%d/g%d/%d/%s/heuristic/params.csv'%(log_dir,v,gid,tid,method),'w') as f:
            f.write('rate;proc\n')
            f.write('%d;%s\n'%(int(rate),','.join([str(p) for p in processing_intervals])))
          #write predicted path latencies
          with open('%s/v%d/g%d/%d/%s/heuristic/prediction.csv'%(log_dir,v,gid,tid,method),'w') as f:
            f.write('path,latency\n')
            for path,l in path_latencies.items():
              f.write('%s,%f\n'%(path,l))
          #write placement
          with open('%s/v%d/g%d/%d/%s/heuristic/placement.csv'%(log_dir,v,gid,tid,method),'w') as f:
            f.write('node;vertices\n')
            for x in range(1,len(placement)+1):
              f.write('bbb%d;%s\n'%(x,','.join([str(x) for x in placement['bbb%d'%(x)]])))
