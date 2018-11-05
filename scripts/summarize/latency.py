import numpy as np
import pandas as pd
import os,argparse

#Returns a map from sink_vertex to latency:{vertex: {avg:ms,90th:ms}} 
def latency(log_dir):
  #collect sink files
  sink_files=[]
  for f in os.listdir(log_dir):
    if(os.path.isfile(os.path.join(log_dir,f)) and f.startswith('v')):
      sink_files.append(log_dir+'/'+f)
  
  sink_latency_map={}
  for f in sink_files:
    sink_vertex=f.rpartition('/')[2].split('.')[0]
    data=pd.read_csv(f,delimiter=',',names=['vid','sample_id','latency'])
    #ignore 10% of initial data samples 
    idx=len(data)//10
    latency_avg=np.mean(data[idx:]['latency'])

    sorted_latency_vals=np.sort(data[idx:]['latency'])
    latency_90th= np.percentile(sorted_latency_vals,90)

    sink_latency_map[sink_vertex]={'avg':latency_avg,\
      '90th':latency_90th}
    
  return sink_latency_map

#Summarizes latency per sink vertex
def process(log_dir,output_dir):
  sink_latency_map=latency(log_dir)
 
  #ensure output directory exists
  if not os.path.exists(output_dir):
    os.makedirs(output_dir)

  with open('%s/summary_latency.csv'%(output_dir),'w') as f:
    f.write('sink,avg(ms),90th(ms)\n')
    for sink,latency_map in sink_latency_map.items():
      f.write('%s,%f,%f\n'%(sink,latency_map['avg'],latency_map['90th']))

if __name__=="__main__":
  parser=argparse.ArgumentParser(description='script to summarize average and 90th percentile latency per sink for an experiment run')
  parser.add_argument('-log_dir',help='log directory containing raw  data',required=True)
  parser.add_argument('-output_dir',help='log directory where summary stats should be saved',required=True)
  args=parser.parse_args()

  process(args.log_dir,args.output_dir)
