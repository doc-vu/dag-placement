import numpy as np
import random,itertools,argparse

def fan_in_fan_out(v,in_deg,out_deg):
  vertices=set()
  v_out_edges={}
  while len(vertices)<v:
    current_vertex_count=len(vertices)

    if random.uniform(0,1) < .5: #fan-out phase
      #get vertex with minimum number of outgoing edges
      if current_vertex_count>0:
        curr_v=np.argmin([len(v_out_edges[i]) for i in range(current_vertex_count)])
        diff=out_deg-len(v_out_edges[curr_v])
        #add a random number of new vertices 
        for i in range(min(random.randint(1,diff),v-current_vertex_count)):
          vertices.add(current_vertex_count+i)
          v_out_edges[current_vertex_count+i]=[]
          v_out_edges[curr_v].append(current_vertex_count+i)
      else:
        #there are no vertices in the graph. Add the first vertex
        vertices.add(0)
        v_out_edges[0]=[]
      
    else: #fan-in phase
      #find set of vertices with out-degree less than out_deg
      candidate_vertices=[x for x,edges in v_out_edges.items() if len(edges)<out_deg]
      #get a subset of at most in-deg vertices from the candidate vertices
      for i in range(in_deg,0,-1):
        subsets=list(itertools.combinations(candidate_vertices,i))
        if len(subsets)>0:
          #select a subset
          subset=subsets[0]
          #add a new vertex
          vertices.add(current_vertex_count) 
          v_out_edges[current_vertex_count]=[]
          for x in subset:
            v_out_edges[x].append(current_vertex_count)
          break
  adj=np.zeros((v,v))
  for x,edges in v_out_edges.items():
    for e in edges: 
      adj[x][e]=1

  #source vertices have a column of all zeros, i.e., no in-coming edge
  source_vertices= np.where(~adj.any(axis=0))[0]
  #sink vertices have a row of all zeros, i.e., no out-going edge
  sink_vertices= np.where(~adj.any(axis=1))[0]

  #ensure that there are no direct edges between source and sink vertices
  for i in source_vertices:
    for j in sink_vertices:
      adj[i][j]=0
  return adj

if __name__=="__main__":
  parser=argparse.ArgumentParser(description='creates random fan-in fan-out graph given the number of vertices, maximum in-degree and max out-degree')
  parser.add_argument('-n',help='number of vertices',type=int,required=True)
  parser.add_argument('-ind',help='maximum in-degree',type=int,required=True)
  parser.add_argument('-outd',help='maximum out-degree',type=int,required=True)
  args=parser.parse_args()

  print(fan_in_fan_out(args.n,args.ind,args.outd))
