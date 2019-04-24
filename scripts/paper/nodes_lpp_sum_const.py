import os
import numpy as np

vcounts=[7,8]
gids={7:[1,2,3],
8:[3,4,5],
}

network_cost=20
log_dir='/home/shweta/workspace/research/dag-placement/log/greedy'

nodes_used={}

for method in ['lpp','sum','const']:
  counts=[]
  for vcount in vcounts:
    for gid in gids[vcount]:
      count=0
      with open('%s/v%d/g%d/1/%s/heuristic/placement.csv'%(log_dir,vcount,gid,method),'r') as f:
        next(f)
        for line in f:
          count+=1
      counts.append(count)
  nodes_used[method]=counts

print(nodes_used)
