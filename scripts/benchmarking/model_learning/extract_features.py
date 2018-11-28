import os,argparse
def extract_features(k,count,node):
  with open('log/model_learning/features/k%d.csv'%(k),'w') as of, open('log/model_learning/parameters/k%d'%(k),'r') as inf:
    of.write('foreground_proc,foreground_rate,foreground_load,background_sum_proc,background_sum_rate,background_sum_load,cpu,foreground_latency_90th\n') #write header
    next(inf) #skip header
    for i in range(count):
      with open('log/model_learning/k%d/%d/summary/summary_util.csv'%(k,i+1),'r') as uf:
        for line in uf:
          if line.startswith(node):
            cpu=line.strip().split(',')[1]
      vertex_parameters=next(inf).strip().split(',')
      for v in range(k):
        proc,rate=vertex_parameters[v].split(':')
        foreground_load=(int(proc)*int(rate))/1000.0
        sum_background_proc=sum([int(vertex_parameters[m].split(':')[0]) for m in range(k) if m!=v])
        sum_background_rate=sum([int(vertex_parameters[m].split(':')[1]) for m in range(k) if m!=v])
        sum_background_load=sum([int(vertex_parameters[m].split(':')[0])*int(vertex_parameters[m].split(':')[1])/1000.0 for m in range(k) if m!=v])
        with open('log/model_learning/k%d/%d/summary/summary_g%d.csv'%(k,i+1,v+1),'r') as lf:
          next(lf)
          latency_90th=next(lf).strip().split(',')[2]
        of.write('%s,%s,%f,%d,%d,%f,%s,%s\n'%(proc,rate,foreground_load,sum_background_proc,sum_background_rate,sum_background_load,cpu,latency_90th))
        

if __name__=="__main__": 
  parser=argparse.ArgumentParser(description='')
  parser.add_argument('-k',help='degree of colocation',required=True,type=int)
  parser.add_argument('-count',help='number of experiment runs',required=True,type=int)
  parser.add_argument('-node',help='node hosting vertices under examination',required=True)
  args=parser.parse_args()
  extract_features(args.k,args.count,args.node)
