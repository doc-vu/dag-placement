import numpy as np
def find_incoming_rate_for_vertex(vertex,dag,selectivity,rate):
  def compute(v):
    indeg= np.nonzero(dag[:,v])[0]
    if len(indeg)==0: 
      return rate
    else:
      return sum([selectivity[x]*compute(x) for x in indeg])
  return compute(vertex)

def find_all_paths_for_vertex(vertex,dag):
  def compute(v):
    out_deg=np.nonzero(dag[v,:])[0]
    if len(out_deg)==0:
      return [[v]]
    else:
      partial_paths=[]
      for x in out_deg:
        forward_paths=compute(x)
        for p in forward_paths:
          partial_paths.append([v]+p)
      return partial_paths

  return compute(vertex)

def find_all_paths(dag):
  paths=[]
  source_vertices=np.where(~dag.any(axis=0))[0]
  for v in source_vertices:
    for p in find_all_paths_for_vertex(v,dag):
      paths.append(p)
  return paths

adj=np.array([[0,1,1,0,0],\
[0,0,1,1,0],\
[0,0,0,1,0],\
[0,0,0,0,1],\
[0,0,0,0,0]]
)
selectivity=[.5,2,4,5,1]
print(find_all_paths(adj))
