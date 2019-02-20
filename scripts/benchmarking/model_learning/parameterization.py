import argparse,random
import pandas as pd

def configurations(colocation,count,output_file):
  #processing_intervals=[10,20,30,40]
  #msr={10:82,20:36,30:31,40:24}
  processing_intervals=[1,5,10,15,20,25,30]
  max_publishing_rate=20
  selectivity=[.5,1]
  #cpu={}
  #for proc in processing_intervals:
  #  cpu[proc]=pd.read_csv('/home/shweta/workspace/research/dag-placement/plots/dds/msr_sleep/data/summary/p%d.csv'%(proc),\
  #    names=['rate','avg_latency(avg)','avg_latency(std)','90th_latency(avg)','90th_latency(std)','cpu(avg)','cpu(std)'],skiprows=1,delimiter=',')
    
  combinations=set()
  while len(combinations)<count:
    print(len(combinations))
    combination=''
    sum_cpu=0
    for k in range(colocation):
      proc=processing_intervals[random.randint(0,len(processing_intervals)-1)]
      rate=random.randint(1,max_publishing_rate)
      sel=selectivity[random.randint(0,len(selectivity)-1)]
      #sum_cpu+=float(cpu[proc][cpu[proc]['rate']==rate]['cpu(avg)'])
      combination=combination + '%d:%d:%.1f,'%(proc,rate,sel)
    combinations.add(combination.rstrip(','))
  
  with open(output_file,'w') as f:
    f.write(','.join(['p%d:r%d:s%d'%(i+1,i+1,i+1) for i in range(colocation)])+'\n')
    for pattern in combinations:
      f.write(pattern +'\n')

      

if __name__=="__main__":
  parser=argparse.ArgumentParser(description='script for creating dataset for k-vertex colocation model learning')
  parser.add_argument('-k',help='degree of colocation of vertices',required=True,type=int)
  parser.add_argument('-count',help='number of test parameterizations to create',required=True,type=int)
  parser.add_argument('-output_file',help='output file path',required=True)
  args=parser.parse_args()

  configurations(args.k,args.count,args.output_file)
