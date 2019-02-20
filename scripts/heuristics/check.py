import pandas as pd
import os
#import os
#for idx in [1,7,9,10,16,19,25,31,39]:
#  if not os.path.exists('log/chaining_vs_single/single/%d/graphs'%(idx)):
#    os.makedirs('log/chaining_vs_single/single/%d/graphs'%(idx))
#  with open('log/heuristics/greedy/v3/g4/run1/%d/graphs/g4.txt'%(idx),'r') as cf:
#    params={}
#    next(cf) #skip header
#    for line in cf:
#      if line.startswith('localhost'):
#        continue
#      node,vertex,subscription,publication,\
#      selectivity,input_rate,sinks,sources,vertices,\
#      publication_rate,processing_interval=line.rstrip().split(';')
#      vid=int(vertex.split('_')[1][1:])
#      params[vid]='%s,%s,%s'%(processing_interval,publication_rate,selectivity)
#    for vid in range(1,4):
#      with open('log/chaining_vs_single/single/%d/graphs/g%d.txt'%(idx,vid),'w') as of:
#        of.write('node;vertex;subscription;publication;selectivity;input_rate;sinks;sources;vertices;publication_rate;processing_interval\n')
#        proc,rate,sel=params[vid].split(',')
#        of.write('localhost;g%d_v0;;g%d_e01;1.000000;1.000000;1;1;3;%s;-1\n'%(vid,vid,rate))
#        of.write('bbb-a702;g%d_v1;g%d_e01;g%d_e12;%s;1.000000;1;1;3;%s;%s\n'%(vid,vid,vid,sel,rate,proc))
#        of.write('localhost;g%d_v2;g%d_e12;;1.000000;%s;1;1;3;%s;-1\n'%(vid,vid,sel,rate))
#with open('comparison.csv','w') as f:
#  f.write('tid,actual,prediction,max,sum\n')
#  for idx in range(1,41):
#    if idx==33:
#      continue
#    with open('log/heuristics/greedy/v3/g4/run1/%d/summary/summary_g4.csv'%(idx),'r') as af:
#      next(af)
#      sink,avg,p_90th=next(af).rstrip().split(',')
#
#    data=pd.read_csv('log/heuristics/greedy/v3/g4/run1/%d/predictions/vertices.txt'%(idx),\
#      names=['vid','latency'],delimiter=';')
#    ppl=sum(data['latency'])
#    m=max(data['latency'])
#
#    with open('log/heuristics/greedy/v3/g4/run1/%d/graphs/g4.txt'%(idx),'r') as gf:
#      next(gf) #skip header
#      sum_proc=0
#      for line in gf:
#        p=int(line.rstrip().split(';')[-1])
#        if p==1:
#          p=5
#        if p>0:
#          sum_proc+=p
#
#    f.write('%d,%s,%f,%f,%d\n'%(idx,p_90th,ppl,m,sum_proc))

#log_dir='/home/shweta/workspace/research/dag-placement/log/linear_chain_overhead/alternating'
#for idx in range(1,13):
#  if not os.path.exists('%s/%d/graphs'%(log_dir,idx)):
#    os.makedirs('%s/%d/graphs'%(log_dir,idx))
#  with open('%s/%d/graphs/g1.txt'%(log_dir,idx),'w') as wf:
#    wf.write('node;vertex;subscription;publication;selectivity;input_rate;sinks;sources;vertices;publication_rate;processing_interval\n')
#    wf.write('localhost;g1_v0;;g1_e01;1.000000;1.000000;1;1;%d;10;-1\n'%(idx+2))
#    wf.write('localhost;g1_v%d;g1_e%d%d;;1.000000;1.000000;1;1;%d;10;-1\n'%(idx+1,idx,idx+1,idx+2))
#    for i in range(1,idx+1):
#      if (i%2==0): 
#        wf.write('bbb-a702;g1_v%d;g1_e%d%d;g1_e%d%d;1.000000;1.000000;1;1;%d;10;10\n'%(i,i-1,i,i,i+1,idx+2))
#      else: 
#        wf.write('bbb-a702;g1_v%d;g1_e%d%d;g1_e%d%d;1.000000;1.000000;1;1;%d;10;5\n'%(i,i-1,i,i,i+1,idx+2))

import random
processing_intervals=[1,5,10,15,20,25,30]
log_dir='/home/shweta/workspace/research/dag-placement/log/linear_chain_overhead/context_switch_3'
for i in range(6):
  selected_intervals=[]
  while len(selected_intervals)<6:
    p=processing_intervals[random.randint(0,len(processing_intervals)-1)]
    selected_intervals.append(p)
  if sum(selected_intervals)<200:
    print(i+1)
    if not os.path.exists('%s/%d/graphs'%(log_dir,i+1)):
      os.makedirs('%s/%d/graphs'%(log_dir,i+1))
    with open('%s/%d/graphs/g1.txt'%(log_dir,i+1),'w') as wf:
      wf.write('node;vertex;subscription;publication;selectivity;input_rate;sinks;sources;vertices;publication_rate;processing_interval\n')
      wf.write('localhost;g1_v0;;g1_e01;1.000000;1.000000;1;1;5;5;-1\n')
      wf.write('localhost;g1_v4;g1_e34;;1.000000;1.000000;1;1;5;5;-1\n')
      for idx in range(1,4):
        wf.write('bbb-ed97;g1_v%d;g1_e%d%d;g1_e%d%d;1.000000;1.000000;1;1;5;5;%d\n'%(idx,idx-1,idx,idx,idx+1,selected_intervals[idx-1]))

    with open('%s/%d/graphs/g2.txt'%(log_dir,i+1),'w') as wf:
      wf.write('node;vertex;subscription;publication;selectivity;input_rate;sinks;sources;vertices;publication_rate;processing_interval\n')
      wf.write('localhost;g2_v0;;g2_e01;1.000000;1.000000;1;1;5;1;-1\n')
      wf.write('localhost;g2_v4;g2_e34;;1.000000;1.000000;1;1;5;1;-1\n')
      for idx in range(1,4):
        wf.write('bbb-ed97;g2_v%d;g2_e%d%d;g2_e%d%d;1.000000;1.000000;1;1;5;1;%d\n'%(idx,idx-1,idx,idx,idx+1,selected_intervals[idx-1+3]))
