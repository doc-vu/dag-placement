import numpy as np
import random,argparse

def erdos_renyi(n,p=None):
  if not p:
    #choose p > (1+epsilon)log(n)/n, so that\
    # there are no isolated vertices with high probability 
    p=np.log(n)/n

  def generate_graph(n,p):
    adj=np.zeros((n,n)) 
    for i in range(n):
      for j in range(i):
        if random.uniform(0,1)<p:
          adj[i][j]=1

    #source vertices have a column of all zeros, i.e., no in-coming edge
    source_vertices= np.where(~adj.any(axis=0))[0]
    #sink vertices have a row of all zeros, i.e., no out-going edge
    sink_vertices= np.where(~adj.any(axis=1))[0]

    #ensure that there are no direct edges between source and sink vertices
    for i in source_vertices:
      for j in sink_vertices:
        adj[i][j]=0
    return adj

  adj=generate_graph(n,p)
  while True:
    #re-generate the random graph until there are no disconnected components
    components=connected_components(adj)
    if len(components)==1:
      break
    else:
      adj=generate_graph(n,p)
  return adj

def connected_components(adj):
  vertices=set([i for i in range(adj.shape[0])])
  connected_components=[]
  def connected_component(v):
    seen=set()
    component=set([v])
    while not seen==component:
      diff=component-seen
      i=diff.pop()
      seen.add(i)
      outgoing_edges=np.nonzero(adj[i,:])[0]
      incoming_edges=np.nonzero(adj[:,i])[0]
      component.update(outgoing_edges)
      component.update(incoming_edges)
    return component
    
  while len(vertices)>0:
    v=vertices.pop()
    component=connected_component(v)
    connected_components.append(component)
    vertices=vertices-component

  return connected_components
    

if __name__=="__main__":
  parser=argparse.ArgumentParser(description='creates random erdos-renyi graph given the number of vertices and probability of edge formation')
  parser.add_argument('-n',help='number of vertices',type=int,required=True)
  parser.add_argument('--p',help='probability of edge formation',type=float)
  args=parser.parse_args()

  print(erdos_renyi(args.n,args.p))
