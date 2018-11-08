import numpy as np
import pandas as pd
import os,argparse

#Returns a map from hostname to utilization metrics: { hostname: { cpu:%,iowait:% mem:% }}
def process_util(log_dir):
  #collect util files
  util_files=[]
  for f in os.listdir(log_dir):
    if(os.path.isfile(os.path.join(log_dir,f)) and f.startswith('util')):
      util_files.append(log_dir+'/'+f)
  
  host_util_map={}
  for f in util_files:
    print(f)
    #extract host name from file name
    hostname=f.rpartition('/')[2].split('_')[1].split('.')[0]
    data=pd.read_csv(f,delimiter=';',\
      names=['hostname','interval','timestamp',\
      'cpu','user','nice','sys','iowait','steal','idle',\
      'free','used','per','buffer','cached','commit','%commit',\
      'active','inactive','dirty'],skiprows=1)
   
    avg_cpu=np.mean(data['user'] + data['sys'])
    avg_iowait=np.mean(data['iowait'])
    avg_mem=((497328-np.mean(data['free']+data['buffer']+data['cached']))*100)/497328
   
    host_util_map[hostname]= {'cpu':avg_cpu,\
      'iowait':avg_iowait,\
      'mem':avg_mem} 
  return host_util_map

#Returns a map from hostname to nw utilization: {hostname: kB/s}      
def process_nw(log_dir):
  #collect nw files
  nw_files=[]
  for f in os.listdir(log_dir):
    if(os.path.isfile(os.path.join(log_dir,f)) and f.startswith('nw')):
      nw_files.append(log_dir+'/'+f)

  #map for host to avg network usage 
  host_nw_map={}
  for f in nw_files:
    print(f)
    #extract host name from file name
    hostname=f.rpartition('/')[2].split('_')[1].split('.')[0]
    data=pd.read_csv(f,delimiter=';',header=None,skiprows=1)
    idx=data.columns[(data=='eth0').all()][0]
   
    #add average network usage to host mapping- rxkB/s+txkB/s
    host_nw_map[hostname]=np.mean(data.iloc[:,idx+3]+data.iloc[:,idx+4])
  return host_nw_map

#Summarizes per-host cpu,memory and nw utilization 
def process(log_dir,output_dir):
  host_util_map=process_util(log_dir)
  host_nw_map=process_nw(log_dir)

  #ensure output directory exists
  if not os.path.exists('%s'%(output_dir)):
    os.makedirs('%s'%(output_dir))
  with open('%s/summary_util.csv'%(output_dir),'w') as f: 
    f.write('hostname,avg_cpu(%),avg_iowait(%),avg_mem(%),avg_nw(kB/sec)\n')
    for hostname in host_util_map.keys():
      f.write('%s,%f,%f,%f,%f\n'%(hostname,\
        host_util_map[hostname]['cpu'],\
        host_util_map[hostname]['iowait'],\
        host_util_map[hostname]['mem'],\
        host_nw_map[hostname]))

if __name__=="__main__":
  parser=argparse.ArgumentParser(description='script to summarize per-host utilization metrics for an experiment run')
  parser.add_argument('-log_dir',help='log directory containing raw utilization data',required=True)
  parser.add_argument('-output_dir',help='log directory where utilization summary should be saved',required=True)
  args=parser.parse_args()

  process(args.log_dir,args.output_dir)
