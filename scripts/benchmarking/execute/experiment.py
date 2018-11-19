from kazoo.client import KazooClient
from kazoo.recipe.watchers import ChildrenWatch
from kazoo.protocol.states import EventType
from kazoo.recipe.barrier import Barrier
import os,time,subprocess,re,sys
sys.path.insert(0, './scripts/summarize')
import latency,util

class Experiment(object):
  def __init__(self,zk_connector,config_dir,remote_log_dir,local_log_dir,\
    publication_rate,processing_interval,execution_time,zmq):
    #stash experiment parameters
    self._config_dir=config_dir
    self._remote_log_dir=remote_log_dir
    self._local_log_dir=local_log_dir
    self._publication_rate=publication_rate
    self._processing_interval=processing_interval
    self._execution_time=execution_time
    self._zmq=zmq
    self._localhost_log_dir='/home/shweta/log'

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
  
    time.sleep(5)

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
            self.dags[graph_id]={'vertices': int(parts[-1]),
              'sinks':int(parts[-3])}
    print(self.dags)

  def configure_zk(self):
    #clean /joined, /barriers, /finished paths
    if self._zk.exists('/joined'):
      self._zk.delete('/joined',recursive=True)
    if self._zk.exists('/exited'):
      self._zk.delete('/exited',recursive=True)
    if self._zk.exists('/finished'):
      self._zk.delete('/finished',recursive=True)
    if self._zk.exists('/barriers'):
      self._zk.delete('/barriers',recursive=True)

    #create barrier paths and barriers
    self._zk.ensure_path('/barriers/start')
    self._zk.ensure_path('/barriers/end')
    self.start_barrier=Barrier(client=self._zk,path='/barriers/start')
    self.end_barrier=Barrier(client=self._zk,path='/barriers/end')

    #create graph_ids under /joined and /finished
    for graph_id in self.dags.keys():
      self._zk.ensure_path('/joined/%s'%(graph_id))
      self._zk.ensure_path('/exited/%s'%(graph_id))
      self._zk.ensure_path('/finished/%s'%(graph_id))

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
            if (self.joined_count==len(self.dags)):
              self.start_ts=int(time.time()*1000)
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
        path='/joined/%s'%(graph_id),\
        func=_joined_endpoint_listener,send_event=True)
      ChildrenWatch(client=self._zk,\
        path='/finished/%s'%(graph_id),\
        func=_finished_endpoint_listener,send_event=True)

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
      self.controller=subprocess.Popen(['java','-cp','build/libs/dag-placement.jar','edu.vanderbilt.kharesp.dagPlacement.zmq.Controller'])
    else:
      self.controller=subprocess.Popen(['java','-cp','build/libs/dag-placement.jar','edu.vanderbilt.kharesp.dagPlacement.dds.Controller'])
 
  def execute(self):
    for graph_id in self.dags.keys():    
      with open('%s/graphs/%s.txt'%(self._config_dir,graph_id),'r') as inp:
        next(inp) #skip header
        for line in inp:
          node=line.split(';')[0]
          vertex_description=line.partition(';')[2]
         
          if node!='localhost': 
            subprocess.check_call(['ansible-playbook','playbooks/vertex.yml',\
              '--limit',node,\
              "--extra-vars=graph_id=%s \
              vertex_description='%s' \
              publication_rate=%d \
              execution_interval=%d \
              log_dir=%s \
              processing_interval=%d \
              zmq=%d"%(graph_id,\
              re.escape(vertex_description.strip()),\
              self._publication_rate,\
              self._execution_time,\
              '%s/%s'%(self._remote_log_dir,graph_id),\
              self._processing_interval,\
              self._zmq)])
          else:
            subprocess.check_call(['ansible-playbook','playbooks/vertex.yml',\
              '--limit','localhost',\
              '--connection','local',\
              "--extra-vars=graph_id=%s \
              vertex_description='%s' \
              scripts_dir='/home/shweta/workspace/research/dag-placement/scripts/remote' \
              publication_rate=%d \
              execution_interval=%d \
              log_dir=%s \
              processing_interval=%d \
              zmq=%d"%(graph_id,\
              re.escape(vertex_description.strip()),\
              self._publication_rate,\
              self._execution_time,\
              '%s/%s'%(self._localhost_log_dir,graph_id),\
              self._processing_interval,\
              self._zmq)])

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
    for graph_id in self.dags.keys():
      if not os.path.exists('%s/data/%s/dag'%(self._local_log_dir,graph_id)):
        os.makedirs('%s/data/%s/dag'%(self._local_log_dir,graph_id))
      
      subprocess.check_call(['ansible-playbook','playbooks/copy2.yml',\
            '--limit',','.join([x for x in self.nodes if x!='localhost']),\
            "--extra-vars=src_dir=%s/%s/dag/ \
            dest_dir=%s/data/%s/dag/"%(self._remote_log_dir,graph_id,self._local_log_dir,graph_id)])
      if 'localhost' in self.nodes:
        subprocess.check_call(['ansible-playbook','playbooks/copy2.yml',\
              '--limit','localhost',\
              '--connection','local',\
              "--extra-vars=src_dir=%s/%s/dag/ \
              dest_dir=%s/data/%s/dag/"%(self._localhost_log_dir,graph_id,self._local_log_dir,graph_id)])

    subprocess.check_call(['ansible-playbook','playbooks/copy2.yml',\
      '--limit',','.join([x for x in self.nodes if x!='localhost']),\
      "--extra-vars=src_dir=%s/util/ \
      dest_dir=%s/util/"%(self._remote_log_dir,self._local_log_dir)])
    
    if 'localhost' in self.nodes:
      subprocess.check_call(['ansible-playbook','playbooks/copy2.yml',\
        '--limit','localhost',\
        '--connection','local',\
        "--extra-vars=src_dir=%s/util/ \
        dest_dir=%s/util/"%(self._localhost_log_dir,self._local_log_dir)])

  def summarize(self):
    for graph_id in self.dags.keys():
      latency.process('%s/data/%s/dag'%(self._local_log_dir,graph_id),graph_id,'%s/summary'%(self._local_log_dir))
    util.process('%s/util'%(self._local_log_dir),'%s/summary'%(self._local_log_dir))

  def cleanup(self):
    self.controller.wait()
    self._zk.stop()
    
if __name__=="__main__":
  processing_intervals=[10]
  publication_rates=[1]
  for config in range(10,15):
    for proc in processing_intervals:
      for rate in publication_rates:
        if not os.path.exists('/home/shweta/workspace/research/dag-placement/log/manual-configurations/data/config%d/p%d/r%d'%(config,proc,rate)):
          os.makedirs('/home/shweta/workspace/research/dag-placement/log/manual-configurations/data/config%d/p%d/r%d'%(config,proc,rate))

        Experiment('129.59.105.159:2181',
          '/home/shweta/workspace/research/dag-placement/log/manual-configurations/configurations/config%d'%(config),
          '/home/riaps/workspace/dag-placement/log',
          '/home/shweta/workspace/research/dag-placement/log/manual-configurations/data/config%d/p%d/r%d'%(config,proc,rate),rate,proc,300,False).run()
