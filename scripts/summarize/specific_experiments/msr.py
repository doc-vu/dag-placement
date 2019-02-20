import os
import pandas as pd
import numpy as np
output_dir='plots/dds/msr_sleep/data'
input_dir='log/msr_sleep'
processing_intervals=[10,20,30,40]
runs=[1,2,3]

for runid in runs:
  for proc in processing_intervals:
    with open('%s/run%d/p%d.csv'%(output_dir,runid,proc),'w') as op:
      op.write('rate,latency(avg),latency(90th),cpu\n')
      rates=os.listdir('%s/run%d/p%d'%(input_dir,runid,proc))
      for r in range(len(rates)):
        with open('%s/run%d/p%d/r%d/summary/summary_g1.csv'%(input_dir,runid,proc,r+1),'r') as inp: 
          next(inp) #skip header
          sink,lavg,l90th=next(inp).strip().split(',')
  
        with open('%s/run%d/p%d/r%d/summary/summary_util.csv'%(input_dir,runid,proc,r+1),'r') as inp: 
          for line in inp:
            if line.startswith('bbb'):
              cpu=line.split(',')[1]
              break
        op.write('%d,%s,%s,%s\n'%(r+1,lavg,l90th,cpu))

for proc in [10,20,30,40]:
  data={}
  for runid in runs:
    data['run%d'%(runid)]=pd.read_csv('%s/run%d/p%d.csv'%(output_dir,runid,proc),names=['rate','avg','90th','cpu'],skiprows=1,delimiter=',')

  min_len=min([len(data['run%d'%(runid)]) for runid in runs])

  avg_latency=pd.DataFrame(data['run%d'%(runs[0])]['avg'][:min_len])
  for runid in runs[1:]:
    avg_latency=pd.concat([avg_latency,data['run%d'%(runid)]['avg'][:min_len]],axis=1)

  avg_90th=pd.DataFrame(data['run%d'%(runs[0])]['90th'][:min_len])
  for runid in runs[1:]:
    avg_90th=pd.concat([avg_90th,data['run%d'%(runid)]['90th'][:min_len]],axis=1)

  avg_cpu=pd.DataFrame(data['run%d'%(runs[0])]['cpu'][:min_len])
  for runid in runs[1:]:
    avg_cpu=pd.concat([avg_cpu,data['run%d'%(runid)]['cpu'][:min_len]],axis=1)

  temp=pd.concat([data['run%d'%(runs[0])]['rate'][:min_len].to_frame(name='rate'),\
    np.mean(avg_latency,axis=1).to_frame(name='avg_latency(avg)'),\
    np.std(avg_latency,axis=1).to_frame(name='avg_latency(std)'),\
    np.mean(avg_90th,axis=1).to_frame(name='90th_latency(avg)'),\
    np.std(avg_90th,axis=1).to_frame(name='90th_latency(std)'),\
    np.mean(avg_cpu,axis=1).to_frame(name='cpu(avg)'),\
    np.std(avg_cpu,axis=1).to_frame(name='cpu(std)'),\
    ],axis=1)
  temp.to_csv('%s/summary/p%d.csv'%(output_dir,proc),index=False)
