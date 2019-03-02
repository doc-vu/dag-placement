import threading,subprocess,os
from kazoo.client import KazooClient
from kazoo.retry import RetryFailedError
from kazoo.exceptions import KazooException

class Guarded(object):
  def __init__(self,zk_connector,config_dir,\
    remote_log_dir,local_log_dir,execution_time,domain_id):
    self.zk_connector=zk_connector
    self.config_dir=config_dir
    self.remote_log_dir=remote_log_dir
    self.local_log_dir=local_log_dir
    self.execution_time=execution_time
    self.domain_id=domain_id

    self.zk=KazooClient(hosts=zk_connector)
    self.attempt=0
    self.experiment_trial=0
    self.event=threading.Event()
    self.nodes=set()

    self.execution_failed_flag=False
    self.periodic_check_timer_interval_sec=10
    self.grace_period_sec=1200
    self.max_execution_attempts=(execution_time+self.grace_period_sec)/10
    self.max_experiment_trials=3

  def parse(self):
    graphs=os.listdir('%s/graphs'%(self.config_dir))
    for graph in graphs:
      graph_id=graph.split('.')[0]
      with open('%s/graphs/%s'%(self.config_dir,graph),'r') as inp:
        next(inp) #skip header
        for vertex_description in inp:
          parts=vertex_description.rstrip().split(';')
          self.nodes.add(parts[0])

  def start_periodic_check_timer(self): 
    self.periodic_check_timer=threading.Timer(self.periodic_check_timer_interval_sec,\
      self.check)
    self.periodic_check_timer.start()

  def check(self):
    self.attempt+=1
    print('Attempt:%d'%(self.attempt))
    if (self.attempt>self.max_execution_attempts):
      print('Execution Failed')
      self.execution_failed_flag=True
      self.event.set()
      return
    else:
      self.query_zk()
      self.start_periodic_check_timer()

  def query_zk(self):
    if not self.zk.exists('/dom%d/barriers/end'%(self.domain_id)):
      print('Execution Successful')
      self.event.set()
     
  def reset_variables(self):
    self.attempt=0
    self.execution_failed_flag=False
    self.event.clear()

  def run(self): 
    self.experiment_trial+=1
    self.zk.start()
    self.parse()
    self.reset_variables()
    self.start_periodic_check_timer()

    #start test
    args=['python','scripts/benchmarking/execute/experiment.py',\
      '-zk_connector',self.zk_connector,\
      '-config_dir',self.config_dir,\
      '-remote_log_dir',self.remote_log_dir,\
      '-local_log_dir',self.local_log_dir,\
      '-execution_time','%d'%(self.execution_time),\
      '-domain_id','%d'%(self.domain_id)]
    p= subprocess.Popen(args)

    #wait for test to finish
    self.event.wait()
    #check if execution was successful
    if self.execution_failed_flag:
      print('Experiment trial:%d failed'%(self.experiment_trial))

      #kill started experiment
      p.kill()
      #cancel periodic check timer
      self.periodic_check_timer.cancel()

      #kill all existing processes
      subprocess.check_call(['ansible-playbook','/home/shweta/workspace/research/dag-placement/playbooks/kill.yml',\
      '--limit',','.join([x for x in self.nodes if x!='localhost']),\
      '--extra-vars=domain_id=%d'%(self.domain_id)])
    
      if 'localhost' in self.nodes:
        subprocess.check_call(['ansible-playbook','/home/shweta/workspace/research/dag-placement/playbooks/kill.yml',\
          '--limit','localhost',\
          '--connection','local',\
          '--extra-vars= scripts_dir=/home/shweta/workspace/research/dag-placement/scripts/remote \
          domain_id=%d'%(self.domain_id)])

      #restart experiment
      if self.experiment_trial<self.max_experiment_trials: 
        self.run()
    else:
      print('Experiment trial:%d was successful'%(self.experiment_trial))
      self.periodic_check_timer.cancel()
      p.wait()
    self.zk.stop()
