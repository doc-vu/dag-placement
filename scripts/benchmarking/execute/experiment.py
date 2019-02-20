from kazoo.client import KazooClient
from kazoo.recipe.watchers import ChildrenWatch
from kazoo.protocol.states import EventType
from kazoo.recipe.barrier import Barrier
import os,time,subprocess,re,sys,argparse
sys.path.insert(0, './scripts/summarize')
import latency,util,shutil

class Experiment(object):
  def __init__(self,zk_connector,config_dir,remote_log_dir,local_log_dir,\
    execution_time,zmq,domain_id):
    #stash experiment parameters
    self._zk_connector=zk_connector
    self._config_dir=config_dir
    self._remote_log_dir=remote_log_dir
    self._local_log_dir=local_log_dir
    self._execution_time=execution_time
    self._zmq=zmq
    self._domain_id=domain_id
    self._localhost_log_dir='/home/shweta/log/%d'%(self._domain_id)
    

    #zookeeper client
    self._zk=KazooClient(hosts=zk_connector)
    self._zk.start() 

    #physical nodes 
    self.nodes=set()
    #dags participating in the experiment
    self.dags={}

  def run(self):
    ##parse experiment description
    self.parse()

    #kill pre-existing vertices
    self.kill()

    #setup zk for coordination
    self.configure_zk()
    self.configure_watches()
    
    #clean logs
    self.clean_logs()

    #start zmq/dds controller 
    self.start_controller()

    #start vertices
    self.execute()

    print('Waiting for experiment to finish.')
    #wait for experiment to finish
    self.end_barrier.wait()
  
    #wait for some time 
    time.sleep(30)

    print('Collecting logs')
    #collect system utilization metrics
    self.sysstat() 

    #collect logs
    self.collect_logs()

    #summarize results
    self.summarize()

    ##exit
    self.cleanup()

  def parse(self):
    graphs=os.listdir('%s/graphs'%(self._config_dir))
    for graph in graphs:
      graph_id=graph.split('.')[0]
      with open('%s/graphs/%s'%(self._config_dir,graph),'r') as inp:
        next(inp) #skip header
        for vertex_description in inp:
          parts=vertex_description.rstrip().split(';')
          self.nodes.add(parts[0])
          if graph_id not in self.dags:
            self.dags[graph_id]={'vertices': int(parts[8]),
              'sinks':int(parts[6])}
    print(self.dags)

  def configure_zk(self):
    #clean /joined, /barriers, /finished paths
    if self._zk.exists('/dom%d/joined'%(self._domain_id)):
      self._zk.delete('/dom%d/joined'%(self._domain_id),recursive=True)
    if self._zk.exists('/dom%d/exited'%(self._domain_id)):
      self._zk.delete('/dom%d/exited'%(self._domain_id),recursive=True)
    if self._zk.exists('/dom%d/finished'%(self._domain_id)):
      self._zk.delete('/dom%d/finished'%(self._domain_id),recursive=True)
    if self._zk.exists('/dom%d/barriers'%(self._domain_id)):
      self._zk.delete('/dom%d/barriers'%(self._domain_id),recursive=True)

    #create barrier paths and barriers
    self._zk.ensure_path('/dom%d/barriers/start'%(self._domain_id))
    self._zk.ensure_path('/dom%d/barriers/end'%(self._domain_id))
    self.start_barrier=Barrier(client=self._zk,path='/dom%d/barriers/start'%(self._domain_id))
    self.end_barrier=Barrier(client=self._zk,path='/dom%d/barriers/end'%(self._domain_id))

    #create graph_ids under /joined,/finished and /exited
    for graph_id in self.dags.keys():
      self._zk.ensure_path('/dom%d/joined/%s'%(self._domain_id,graph_id))
      self._zk.ensure_path('/dom%d/exited/%s'%(self._domain_id,graph_id))
      self._zk.ensure_path('/dom%d/finished/%s'%(self._domain_id,graph_id))

  def configure_watches(self):
    self.joined_count=0
    self.finished_count=0
    self.joined={k:0 for k in self.dags.keys()}
    self.finished={k:0 for k in self.dags.keys()}

    def _joined_endpoint_listener(children,event):
      if event and event.type==EventType.CHILD:
        if 'joined' in event.path:
          joined_dag=event.path.split('/')[-1]
          self.joined[joined_dag]=self.joined[joined_dag]+1
          if (self.joined[joined_dag]==self.dags[joined_dag]['vertices']):
            self.joined_count+=1
            print('DAGS:%d have joined'%(self.joined_count))
            if (self.joined_count==len(self.dags)):
              self.start_ts=int(time.time()*1000)
              print('All vertices have joined. Removing start barrier')
              self.start_barrier.remove()
            return False

    def _finished_endpoint_listener(children,event):
      if event and event.type==EventType.CHILD:
        if 'finished' in event.path: 
          finished_dag=event.path.split('/')[-1]
          self.finished[finished_dag]=self.finished[finished_dag]+1
          if (self.finished[finished_dag]==self.dags[finished_dag]['sinks']):
            self.finished_count+=1
            if (self.finished_count==len(self.dags)):
              self.end_ts=int(time.time()*1000)
              self.end_barrier.remove()
            return False

    for graph_id in self.dags.keys():
      ChildrenWatch(client=self._zk,\
        path='/dom%d/joined/%s'%(self._domain_id,graph_id),\
        func=_joined_endpoint_listener,send_event=True)
      ChildrenWatch(client=self._zk,\
        path='/dom%d/finished/%s'%(self._domain_id,graph_id),\
        func=_finished_endpoint_listener,send_event=True)
  
  def kill(self):
    #kill pre-existing vertices
    subprocess.check_call(['ansible-playbook','playbooks/kill.yml',\
      '--limit',','.join([x for x in self.nodes if x!='localhost']),\
      '--extra-vars=domain_id=%d'%(self._domain_id)]) 
    if 'localhost' in self.nodes:
      subprocess.check_call(['ansible-playbook','playbooks/kill.yml',\
        '--limit','localhost',\
        '--connection','local',\
        '--extra-vars= scripts_dir=/home/shweta/workspace/research/dag-placement/scripts/remote \
        domain_id=%d'%(self._domain_id)])

  def clean_logs(self):
    #clean-up remote log directory
    subprocess.check_call(['ansible-playbook','playbooks/clean.yml',\
      '--limit',','.join([x for x in self.nodes if x!='localhost']),\
      '--extra-vars=dir=%s'%(self._remote_log_dir)]) 

    if 'localhost' in self.nodes:
      subprocess.check_call(['ansible-playbook','playbooks/clean.yml',\
        '--limit','localhost',\
        '--connection','local',\
        '--extra-vars=dir=%s'%(self._localhost_log_dir)]) 

  def start_controller(self):
    if self._zmq:
      self.controller=subprocess.Popen(['java','-cp','build/libs/dag-placement.jar','edu.vanderbilt.kharesp.dagPlacement.zmq.Controller',self._zk_connector])
    else:
      self.controller=subprocess.Popen(['java','-cp','build/libs/dag-placement.jar','edu.vanderbilt.kharesp.dagPlacement.dds.Controller',self._zk_connector,'%d'%(self._domain_id)])
 
  def execute(self):
    node_vertex={}
    for graph_id in self.dags.keys():    
      with open('%s/graphs/%s.txt'%(self._config_dir,graph_id),'r') as inp:
        next(inp) #skip header
        for line in inp:
          node=line.split(';')[0]
          vertex_description=line.rstrip().partition(';')[2]
          vid=line.split(';')[1]
          if node in node_vertex:
            node_vertex[node][vid]='%s;%s'%(graph_id,vertex_description)
          else:
            node_vertex[node]={vid:'%s;%s'%(graph_id,vertex_description)}

    for node,vertices in node_vertex.items():
      if node!='localhost':
        subprocess.check_call(['ansible-playbook','playbooks/vertex.yml',\
          '--limit',node,\
          '--extra-vars=map=%s \
          execution_interval=%d \
          log_dir=%s \
          zmq=%d \
          zk_connector=%s \
          domain_id=%d'%(str(vertices).replace(" ",""),\
          self._execution_time,\
          '%s'%(self._remote_log_dir),\
          self._zmq,self._zk_connector,self._domain_id)])
      else: 
        subprocess.check_call(['ansible-playbook','playbooks/vertex.yml',\
          '--limit','localhost',\
          '--connection','local',\
          '--extra-vars=map=%s \
          scripts_dir=/home/shweta/workspace/research/dag-placement/scripts/remote \
          execution_interval=%d \
          log_dir=%s \
          zmq=%d \
          zk_connector=%s \
          domain_id=%d'%(str(vertices).replace(" ",""),\
          self._execution_time,\
          '%s'%(self._localhost_log_dir),\
          self._zmq,self._zk_connector,self._domain_id)])

  def sysstat(self):
    subprocess.check_call(['ansible-playbook','playbooks/sysstat.yml',\
      '--limit',','.join([x for x in self.nodes if x!='localhost']),\
      "--extra-vars=log_dir=%s/util \
      start_ts=%d \
      end_ts=%d"%(self._remote_log_dir,self.start_ts,self.end_ts)])
    if 'localhost' in self.nodes:
      subprocess.check_call(['ansible-playbook','playbooks/sysstat.yml',\
        '--limit','localhost',\
        '--connection','local',\
        "--extra-vars=log_dir=%s/util \
        scripts_dir='/home/shweta/workspace/research/dag-placement/scripts/remote' \
        start_ts=%d \
        end_ts=%d"%(self._localhost_log_dir,self.start_ts,self.end_ts)])

  def collect_logs(self):
    subprocess.check_call(['ansible-playbook','playbooks/copy2.yml',\
          '--limit',','.join([x for x in self.nodes if x!='localhost']),\
          "--extra-vars=src_dir=%s/ \
          dest_dir=%s/ ignore='err'"%(self._remote_log_dir,self._local_log_dir)])
    if 'localhost' in self.nodes:
      subprocess.check_call(['ansible-playbook','playbooks/copy2.yml',\
            '--limit','localhost',\
            '--connection','local',\
            "--extra-vars=src_dir=%s/ \
            dest_dir=%s/ ignore='err'"%(self._localhost_log_dir,self._local_log_dir)])

    files=[f for f in os.listdir(self._local_log_dir) if os.path.isfile('%s/%s'%(self._local_log_dir,f))]
    for graph_id in self.dags.keys():
      if not os.path.exists('%s/data/%s/dag'%(self._local_log_dir,graph_id)):
        os.makedirs('%s/data/%s/dag'%(self._local_log_dir,graph_id))
    if not os.path.exists('%s/util'%(self._local_log_dir)):
      os.makedirs('%s/util'%(self._local_log_dir))
    for file_name in files:
      if file_name.startswith('util') or file_name.startswith('nw'):
        shutil.move('%s/%s'%(self._local_log_dir,file_name),'%s/util/'%(self._local_log_dir))
      for graph_id in self.dags.keys():
        if file_name.startswith(graph_id):
          shutil.move('%s/%s'%(self._local_log_dir,file_name),'%s/data/%s/dag/'%(self._local_log_dir,graph_id))
  #def collect_logs(self):
  #  for graph_id in self.dags.keys():
  #    if not os.path.exists('%s/data/%s/dag'%(self._local_log_dir,graph_id)):
  #      os.makedirs('%s/data/%s/dag'%(self._local_log_dir,graph_id))
  #    
  #    subprocess.check_call(['ansible-playbook','playbooks/copy2.yml',\
  #          '--limit',','.join([x for x in self.nodes if x!='localhost']),\
  #          "--extra-vars=src_dir=%s/%s/dag/ \
  #          dest_dir=%s/data/%s/dag/"%(self._remote_log_dir,graph_id,self._local_log_dir,graph_id)])
  #    if 'localhost' in self.nodes:
  #      subprocess.check_call(['ansible-playbook','playbooks/copy2.yml',\
  #            '--limit','localhost',\
  #            '--connection','local',\
  #            "--extra-vars=src_dir=%s/%s/dag/ \
  #            dest_dir=%s/data/%s/dag/"%(self._localhost_log_dir,graph_id,self._local_log_dir,graph_id)])

  #  subprocess.check_call(['ansible-playbook','playbooks/copy2.yml',\
  #    '--limit',','.join([x for x in self.nodes if x!='localhost']),\
  #    "--extra-vars=src_dir=%s/util/ \
  #    dest_dir=%s/util/"%(self._remote_log_dir,self._local_log_dir)])
  #  
  #  if 'localhost' in self.nodes:
  #    subprocess.check_call(['ansible-playbook','playbooks/copy2.yml',\
  #      '--limit','localhost',\
  #      '--connection','local',\
  #      "--extra-vars=src_dir=%s/util/ \
  #      dest_dir=%s/util/"%(self._localhost_log_dir,self._local_log_dir)])

  def summarize(self):
    for graph_id in self.dags.keys():
      latency.process('%s/data/%s/dag'%(self._local_log_dir,graph_id),graph_id,'%s/summary'%(self._local_log_dir))
    util.process('%s/util'%(self._local_log_dir),'%s/summary'%(self._local_log_dir))

  def cleanup(self):
    self.controller.wait()
    self._zk.stop()

if __name__=="__main__":
  parser=argparse.ArgumentParser(description='script to execute experiment')
  parser.add_argument('-zk_connector',help='zookeeper connector')
  parser.add_argument('-config_dir',help='experiment configuration directory path')
  parser.add_argument('-remote_log_dir',help='remote log directory path')
  parser.add_argument('-local_log_dir',help='local log directory path')
  parser.add_argument('-execution_time',type=int,help='execution interval for an experiment run')
  parser.add_argument('-domain_id',type=int,help='DDS domain ID')
  args=parser.parse_args()

  Experiment(zk_connector=args.zk_connector,\
    config_dir=args.config_dir,\
    remote_log_dir=args.remote_log_dir,\
    local_log_dir=args.local_log_dir,\
    execution_time= args.execution_time,\
    zmq=False,domain_id=args.domain_id).run()
