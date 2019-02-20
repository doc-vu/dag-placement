import os,argparse
def extract_features(k,count,node,log_dir,start_id):
  with open('%s/features/k%d.csv'%(log_dir,k),'w') as of, open('%s/parameters/k%d'%(log_dir,k),'r') as inf:
    of.write('tid,fv,fp,fr,bv,bsp,bsr,bsl,cpu,mem,nw,latency_90th\n') #write header
    for i in range(start_id-1+ count):
      dags=next(inf).strip().split('/')
      if(i<start_id-1):
        continue
      if i==94 or i==118 or i==172 or i==328:
        continue

      #get system utilization metrics for the test run
      with open('%s/data/k%d/%d/summary/summary_util.csv'%(log_dir,k,i+1),'r') as uf:
        for line in uf:
          if line.startswith(node):
            cpu=line.strip().split(',')[1]
            mem=line.strip().split(',')[3]
            nw=line.strip().split(',')[4]

      #get latencies for k colocated linear chains
      latencies={}
      for gid in range(k):
        with open('%s/data/k%d/%d/summary/summary_g%d.csv'%(log_dir,k,i+1,gid+1),'r') as lf:
          next(lf)
          latency_90th=next(lf).strip().split(',')[2]
          latencies['g%d'%(gid+1)]=latency_90th

      #get parameters for the colocated linear chains
      params={}
      for gid in range(k):
        vcount,rate,proc=dags[gid].split(';')
        params['g%d'%(gid+1)]={
          'v': int(vcount),
          'r': int(rate),
          'p': sum([int(x) for x in proc.split(',')]),
          }

      #write features 
      for curr_gid in range(k):
        fv=params['g%d'%(curr_gid+1)]['v']
        fp=params['g%d'%(curr_gid+1)]['p']
        fr=params['g%d'%(curr_gid+1)]['r']
        bv=0;bsp=0;bsr=0;bsl=0
        for gid in range(k):
          if curr_gid==gid:
            continue
          bv+=params['g%d'%(gid+1)]['v']
          bsp+=params['g%d'%(gid+1)]['p']
          bsr+=params['g%d'%(gid+1)]['r']
          bsl+=(params['g%d'%(gid+1)]['p']*params['g%d'%(gid+1)]['r'])/1000.0
        of.write('%d,%d,%d,%d,%d,%d,%d,%f,%s,%s,%s,%s\n'%(i+1,fv,fp,fr,bv,bsp,bsr,bsl,cpu,mem,nw,latencies['g%d'%(curr_gid+1)]))

if __name__=="__main__": 
  parser=argparse.ArgumentParser(description='')
  parser.add_argument('-k',help='degree of colocation',required=True,type=int)
  parser.add_argument('-count',help='number of experiment runs',required=True,type=int)
  parser.add_argument('-node',help='node hosting vertices under examination',required=True)
  parser.add_argument('-log_dir',help='log directory path',required=True)
  parser.add_argument('--start_id',type=int,help='start_id',default=1)
  args=parser.parse_args()
  extract_features(args.k,args.count,args.node,args.log_dir,args.start_id)
