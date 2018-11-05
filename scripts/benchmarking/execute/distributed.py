import argparse,subprocess,re,os


def execute(dag,graph_id,publication_rate,execution_interval,\
  processing_interval,remote_log_dir,local_log_dir):

  #get all vertices in DAG
  with open(dag,'r') as f:
    next(f)#skip header
    vertices=f.readlines()
 
  sinks=int(vertices[0].strip().split(';')[5])
  #clean-up remote log directory
  subprocess.check_call(['ansible-playbook','playbooks/clean.yml',\
    '--limit','all[0:%d]'%(len(vertices)-1),\
    '--extra-vars=dir=%s'%(remote_log_dir)]) 

  #execute each vertex on a separate device
  for idx,vertex_description in enumerate(vertices):
    if idx==(len(vertices)-1):
      subprocess.check_call(['ansible-playbook','playbooks/vertex.yml',\
        '--limit','all[%d]'%(idx),\
        "--extra-vars=detachedMode=false \
        graph_id=%s \
        vertex_description='%s' \
        publication_rate=%d \
        execution_interval=%d \
        log_dir=%s \
        processing_interval=%d"%(graph_id,\
        re.escape(vertex_description.strip()),\
        publication_rate,\
        execution_interval,\
        '%s/%s'%(remote_log_dir,graph_id),\
        processing_interval)])
    else:
      subprocess.check_call(['ansible-playbook','playbooks/vertex.yml',\
        '--limit','all[%d]'%(idx),\
        "--extra-vars=graph_id=%s \
        vertex_description='%s' \
        publication_rate=%d \
        execution_interval=%d \
        log_dir=%s \
        processing_interval=%d"%(graph_id,\
        re.escape(vertex_description.strip()),\
        publication_rate,\
        execution_interval,\
        '%s/%s'%(remote_log_dir,graph_id),\
        processing_interval)])


  #copy log files from remote devices
  while(not verify('%s/%s'%(local_log_dir,graph_id),sinks,len(vertices))):
    #collect all log files
    subprocess.check_call(['ansible-playbook',\
      'playbooks/copy.yml',\
      '--limit',\
      'all[0:%d]'%(len(vertices)-1),\
      '--extra-vars=src_dir=%s/%s dest_dir=%s'%(remote_log_dir,graph_id,local_log_dir)])

#verify if all log files are present
def verify(log_dir,sinks,devices):
  if not os.path.exists('%s'%(log_dir)):
    return False

  files=os.listdir('%s/dag'%(log_dir))
  util_count=0
  nw_count=0
  sink_count=0
  for f in files:
    if f.startswith('util'):
      util_count+=1
    if f.startswith('nw'):
      nw_count+=1
    if f.startswith('v'):
      sink_count+=1
  if ((sink_count==sinks) and (util_count==devices) and (nw_count==devices)):
    return True
  else:
    return False

if __name__=="__main__":
  parser=argparse.ArgumentParser(description='script to execute a dag such that each operator is placed on a separate device')
  parser.add_argument('-dag',help='path to dag description file',required=True)
  parser.add_argument('-graph_id',help='DAG id',required=True)
  parser.add_argument('-publication_rate',type=int,help='rate of publication of data for a source vertex',required=True)
  parser.add_argument('-execution_interval',type=int,help='length of time in seconds for which the DAG should execute',required=True)
  parser.add_argument('-processing_interval',type=int,help='processing interval for each intermediate vertex',required=True)
  parser.add_argument('-remote_log_dir',help='remote log directory where output logs are present',required=True)
  parser.add_argument('-local_log_dir',help='local log directory where output logs should be placed',required=True)
  args=parser.parse_args()

  execute(args.dag,args.graph_id,args.publication_rate,args.execution_interval,\
    args.processing_interval,args.remote_log_dir,args.local_log_dir)
