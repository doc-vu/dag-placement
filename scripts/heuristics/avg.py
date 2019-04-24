import sys,os,shutil
import numpy as np

log_dir='/home/shweta/workspace/research/dag-placement/log/linear_chain_overhead/structure/composition/sub_structure6'
for tid in range(1,2):
  print('\n')
  number_of_graphs=len(os.listdir('%s/1/summary'%(log_dir)))-1
  for gid in range(number_of_graphs):
    l=[]
    for runid in range(1,6):
      with open('%s/%d/summary/summary_g%d.csv'%(log_dir,runid,gid+1),'r') as f:
        next(f)
        for line in f:
          sink,avg,l90th= line.strip().split(',')
          #if 'e56' in sink:
          l.append(float(l90th))
    #print(l)
    print('tid:%d g%d mean:%.2f std:%.2f'%(tid,gid+1,np.mean(l),np.std(l)))
