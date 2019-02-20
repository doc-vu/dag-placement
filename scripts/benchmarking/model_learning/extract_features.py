import os,argparse
def extract_features(k,count,node,log_dir,start_id):
  with open('%s/features/k%d.csv'%(log_dir,k),'w') as of, open('%s/parameters/k%d'%(log_dir,k),'r') as inf:
    of.write('tid,foreground_proc,foreground_rate,foreground_load,foreground_selectivity,background_sum_proc,background_sum_incoming_rate,background_sum_outgoing_rate,background_sum_load,cpu,mem,nw,foreground_latency_90th\n') #write header
    next(inf) #skip header
    for i in range(start_id-1+ count):
      vertex_parameters=next(inf).strip().split(',')
      if(i<start_id-1):
        continue
      with open('%s/data/k%d/%d/summary/summary_util.csv'%(log_dir,k,i+1),'r') as uf:
        for line in uf:
          if line.startswith(node):
            cpu=line.strip().split(',')[1]
            mem=line.strip().split(',')[3]
            nw=line.strip().split(',')[4]
      for v in range(k):
        proc,rate,selectivity=vertex_parameters[v].split(':')
        foreground_load=(int(proc)*int(rate))/1000.0
        sum_background_proc=sum([int(vertex_parameters[m].split(':')[0]) for m in range(k) if m!=v])
        sum_background_incoming_rate=sum([int(vertex_parameters[m].split(':')[1]) for m in range(k) if m!=v])
        sum_background_outgoing_rate=sum([int(vertex_parameters[m].split(':')[1])*float(vertex_parameters[m].split(':')[2]) for m in range(k) if m!=v])
        sum_background_load=sum([int(vertex_parameters[m].split(':')[0])*int(vertex_parameters[m].split(':')[1])/1000.0 for m in range(k) if m!=v])
        with open('%s/data/k%d/%d/summary/summary_g%d.csv'%(log_dir,k,i+1,v+1),'r') as lf:
          next(lf)
          latency_90th=next(lf).strip().split(',')[2]
        of.write('%d,%s,%s,%f,%s,%d,%d,%f,%f,%s,%s,%s,%s\n'%(i+1,proc,rate,foreground_load,selectivity,\
          sum_background_proc,sum_background_incoming_rate,sum_background_outgoing_rate,sum_background_load,cpu,mem,nw,latency_90th))
        

if __name__=="__main__": 
  parser=argparse.ArgumentParser(description='')
  parser.add_argument('-k',help='degree of colocation',required=True,type=int)
  parser.add_argument('-count',help='number of experiment runs',required=True,type=int)
  parser.add_argument('-node',help='node hosting vertices under examination',required=True)
  parser.add_argument('-log_dir',help='log directory path',required=True)
  parser.add_argument('--start_id',type=int,help='start_id',default=1)
  args=parser.parse_args()
  extract_features(args.k,args.count,args.node,args.log_dir,args.start_id)
