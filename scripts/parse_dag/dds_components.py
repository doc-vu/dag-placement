import numpy as np
import os

def dds_components(gid,input_file,output_file):
  adj=np.loadtxt(input_file,dtype=int,delimiter=',') 
  with open(output_file,'w') as f:
    f.write('process;subscription;publication\n')
    for vertex in range(adj.shape[0]):
      outgoing_edges=['%s_e%d%d'%(gid,vertex,x)  for x in np.nonzero(adj[vertex,:])[0]]
      incoming_edges=['%s_e%d%d'%(gid,x,vertex)  for x in np.nonzero(adj[:,vertex])[0]]
      f.write('%s_v%d;%s;%s\n'%(gid,vertex,','.join(incoming_edges),','.join(outgoing_edges)))

if __name__=="__main__":
  for v in range(2,9):
    base_log_dir='dags/fan_in_fan_out/v%d'%(v)
    if not os.path.exists('%s/dds'%(base_log_dir)):
      os.makedirs('%s/dds'%(base_log_dir))
    for g in range(1,2):
      dds_components('v%d_g%d'%(v,g),\
        '%s/adj/v%d_g%d.txt'%(base_log_dir,v,g),\
        '%s/dds/v%d_g%d.txt'%(base_log_dir,v,g))
