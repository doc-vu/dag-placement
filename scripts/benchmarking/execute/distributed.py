import argparse,subprocess,re,os,time,sys
from multiprocessing import Process
sys.path.insert(0, './scripts/benchmarking/execute/zmq')
import coordinator


def execute(dag,graph_id,publication_rate,execution_interval,\
  processing_interval,remote_log_dir,local_log_dir,inventory_file,zmq,zk_connector=None):
  
  #get count of physical nodes present in the test-bed
  nodes=[]
  with open(inventory_file,'r') as f:
    for line in f:
      nodes.append(line.split(' ')[0])

  #get all vertices in DAG
  with open(dag,'r') as f:
    next(f)#skip header
    vertices=f.readlines()
  
  #get count of physical nodes that will be used in the experiment 
  if len(vertices)>len(nodes):
    experiment_node_count=len(nodes)
  else:
    experiment_node_count=len(vertices)
 
  sinks=int(vertices[0].strip().split(';')[5])

  if zmq:
    #start coordinator
    print('ZMQ based implementation')
    coord=coordinator.Coordinate(zk_connector=zk_connector,graph_id=graph_id,vertex_count=len(vertices),sink_count=sinks)
    zk_coordinator=Process(target=coord.run)
    zk_coordinator.start()

  #clean-up remote log directory
  subprocess.check_call(['ansible-playbook','playbooks/clean.yml',\
    '--limit','all[0:%d]'%(experiment_node_count-1),\
    '--inventory',inventory_file,\
    '--extra-vars=dir=%s'%(remote_log_dir)]) 

  #execute each vertex on a separate device
  for idx,vertex_description in enumerate(vertices):
    if idx>=(len(nodes)-1):
      node_id=len(nodes)-1
    else:
      node_id=idx
    if idx==(len(vertices)-1):
      subprocess.check_call(['ansible-playbook','playbooks/vertex.yml',\
        '--limit','all[%d]'%(node_id),\
        '--inventory',inventory_file,\
        "--extra-vars=detachedMode=False \
        graph_id=%s \
        vertex_description='%s' \
        publication_rate=%d \
        execution_interval=%d \
        log_dir=%s \
        processing_interval=%d \
        zmq=%d"%(graph_id,\
        re.escape(vertex_description.strip()),\
        publication_rate,\
        execution_interval,\
        '%s/%s'%(remote_log_dir,graph_id),\
        processing_interval,zmq)])
    else:
      subprocess.check_call(['ansible-playbook','playbooks/vertex.yml',\
        '--limit','all[%d]'%(node_id),\
        '--inventory',inventory_file,\
        "--extra-vars=graph_id=%s \
        vertex_description='%s' \
        publication_rate=%d \
        execution_interval=%d \
        log_dir=%s \
        processing_interval=%d \
        zmq=%d"%(graph_id,\
        re.escape(vertex_description.strip()),\
        publication_rate,\
        execution_interval,\
        '%s/%s'%(remote_log_dir,graph_id),\
        processing_interval,zmq)])


  #copy log files from remote devices
  while(not verify('%s'%(local_log_dir),sinks,experiment_node_count)):
    #collect all log files
    subprocess.check_call(['ansible-playbook',\
      'playbooks/copy2.yml',\
      '--inventory',inventory_file,\
      '--limit',\
      'all[0:%d]'%(experiment_node_count-1),\
      '--extra-vars=src_dir=%s/%s/dag dest_dir=%s/dag/'%(remote_log_dir,graph_id,local_log_dir)])

  if zmq:
    zk_coordinator.join()

#verify if all log files are present
def verify(log_dir,sinks,devices):
  if not os.path.exists('%s/dag'%(log_dir)):
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
    if (sink_count!=sinks):
      print('sink')
    if (util_count!=devices):
      print('util_count')
    if (nw_count!=devices):
      print('nw count')
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
  parser.add_argument('-inventory',help='inventory file of test nodes',required=True)
  parser.add_argument('--zmq',help='flag to specify execution of zmq based implementation',action='store_true',default=False)
  parser.add_argument('--zk_connector',help='zookeeper connector',required='--zmq' in sys.argv)
  args=parser.parse_args()

  execute(args.dag,args.graph_id,args.publication_rate,args.execution_interval,\
    args.processing_interval,args.remote_log_dir,args.local_log_dir,args.inventory,args.zmq,args.zk_connector)
