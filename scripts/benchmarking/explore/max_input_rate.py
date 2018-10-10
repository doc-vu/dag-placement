import sys
sys.path.append('scripts/parse_dag')
import dag_utils
import numpy as np

def max_input_rate(vcount,graphCount,dag_directory):
  max_rate_per_selectivity=[]
  with open('dags/config/selectivity/v%d'%(vcount),'r') as inp:
    for line in inp:
      selectivity=[float(i) for i in line.rstrip().split(',')]
      max_rate_per_graph=[]
      for g in range(graphCount):
        adj=np.loadtxt('%s/v%d/adj/v_%d_g_%d.txt'%(dag_directory,vcount,vcount,g+1),\
          dtype=int,delimiter=',') 
        input_rates=dag_utils.find_incoming_rate(adj,selectivity,1)
        max_rate_per_graph.append(max(input_rates))
      max_rate_per_selectivity.append(max(max_rate_per_graph))
  return max(max_rate_per_selectivity)

if __name__=="__main__": 
  print('\nErdos Renyi:')       
  for vcount in range(4,9):
    print('\tFor vcount:%d maximum input rate:%f'%\
      (vcount,max_input_rate(vcount,5,'dags/erdos_renyi')))

  print('\nFan-In Fan-Out:')       
  for vcount in range(4,9):
    print('\tFor vcount:%d maximum input rate:%f'%\
      (vcount,max_input_rate(vcount,5,'dags/fan_in_fan_out')))
