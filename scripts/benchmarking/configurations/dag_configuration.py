import sys,os
sys.path.append('scripts/parse_dag')
import dag_utils
import numpy as np

def max_input_rate():
  def compute(vcount,graphCount,dag_type):
    max_rate_per_selectivity=[]
    with open('dags/config/selectivity/v%d'%(vcount),'r') as inp:
      for line in inp:
        max_rate_per_graph=[]
        for g in range(graphCount):
          selectivity=[float(i) for i in line.rstrip().split(',')]
          adj=np.loadtxt('dags/%s/v%d/adj/v_%d_g_%d.txt'%(dag_type,vcount,vcount,g+1),\
            dtype=int,delimiter=',') 
          source_vertices=np.where(~adj.any(axis=0))[0]
          sink_vertices=np.where(~adj.any(axis=1))[0]
          for sv in source_vertices:
            selectivity[sv]=1
          for sv in sink_vertices:
            selectivity[sv]=1
          
          input_rates=dag_utils.find_incoming_rate(adj,selectivity,1)
          max_rate_per_graph.append(max(input_rates))
        max_rate_per_selectivity.append(max(max_rate_per_graph))
    return max(max_rate_per_selectivity)
  print('\nErdos Renyi:')       
  for vcount in range(4,9):
    print('\tFor vcount:%d maximum input rate:%f'%\
      (vcount,compute(vcount,5,'erdos_renyi')))

  print('\nFan-In Fan-Out:')       
  for vcount in range(4,9):
    print('\tFor vcount:%d maximum input rate:%f'%\
      (vcount,compute(vcount,5,'fan_in_fan_out')))

def create_configuration(vcount,graphCount,dag_type):
  for g in range(graphCount):
    selectivities=set()
    adj=np.loadtxt('dags/%s/v%d/adj/v_%d_g_%d.txt'%(dag_type,vcount,vcount,g+1),\
      dtype=int,delimiter=',')
    source_vertices=np.where(~adj.any(axis=0))[0]
    sink_vertices=np.where(~adj.any(axis=1))[0]
    with open('dags/config/selectivity/v%d'%(vcount),'r') as sel:
      for idx,line in enumerate(sel):
        selectivity=[float(i) for i in line.rstrip().split(',')]
        for sv in source_vertices:
          selectivity[sv]=1
        for sv in sink_vertices:
          selectivity[sv]=1
        selectivities.add(','.join(['%f'%(s) for s in selectivity]))

    for idx,selectivity_str in enumerate(selectivities):
      selectivity=[float(i) for i in selectivity_str.split(',')]  
      input_rates=dag_utils.find_incoming_rate(adj,selectivity,1)
      #ensure directory:dags/config/input_rate/dag_type/v#/g# exists
      if not os.path.exists('dags/config/input_rate/%s/v%d/g%d'%(dag_type,vcount,g+1)):
        os.makedirs('dags/config/input_rate/%s/v%d/g%d'%(dag_type,vcount,g+1))
      
      with open('dags/%s/v%d/dds/v_%d_g_%d.txt'%(dag_type,vcount,vcount,g+1),'r') as dds,\
        open('dags/config/input_rate/%s/v%d/g%d/v_%d_g_%d_c%d.txt'%(dag_type,vcount,g+1,vcount,g+1,idx+1),'w') as cfg:
        next(dds)#skip header
        cfg.write('vertex;subscription;publication;selectivity;input_rate;sinks;sources;vertices\n')
        for i in range(vcount):
          dds_components=next(dds)
          cfg.write('%s;%f;%f;%d;%d;%d\n'%(dds_components.rstrip(),\
            selectivity[i],input_rates[i],len(sink_vertices),len(source_vertices),vcount))

    
if __name__=="__main__": 
  for vcount in range(4,9):
    create_configuration(vcount,5,'fan_in_fan_out')
