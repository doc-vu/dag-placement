import threading,time,sys,argparse,signal
sys.path.insert(0, './scripts/benchmarking/execute')
import send_mail
from kazoo.client import KazooClient
from kazoo.recipe.watchers import DataWatch
from kazoo.protocol.states import EventType

class Track(object):
  def __init__(self,zk_connector):
    self.start_ts=-1
    self.runid=0
    self.execution_failed=False
    self.zk=KazooClient(hosts=zk_connector)
    self.zk.start()

  def install_listener(self):
    def listener(data,stat,event):
      try:
        if (event and event.type==EventType.CHANGED):
          curr_ts=int(time.time())
          if (self.start_ts > 0):
            elapsed_time= (curr_ts-self.start_ts)/(60.0)
            print('Current runid:%s. Previous experiment run took:%f mins'%(data,elapsed_time))
          self.start_ts=curr_ts
      except Exception as e:
        print(e)
    DataWatch(client=self.zk,path='/runid',func=listener,send_event=True)

  def start_update_timer(self): #sends an update email every 60mins
    self.status_update=threading.Timer(3600,self.update)
    self.status_update.start()

  def start_periodic_check_timer(self): #checks whether the test has failed after 10mins
    self.periodic_check=threading.Timer(600,self.check)
    self.periodic_check.start()

  def start(self):  
    self.install_listener()
    self.start_periodic_check_timer()
    self.start_update_timer()

  def stop(self):
    self.periodic_check.cancel()
    self.status_update.cancel()
    self.zk.stop()

  def check(self):
    value,info=self.zk.get('/runid')
    if(int(value)==self.runid):
      print('\nTest Execution for run-id:%d has failed\n'%(self.runid))
      self.execution_failed=True
      send_mail.Email().send(['kharesp28@gmail.com'],
        'Execution Failure','Check execution')
    else:
      self.runid=int(value)
    self.start_periodic_check_timer()
  
  def update(self):
    value,info=self.zk.get('/runid')
    print('\nSending status update for runid:%s\n'%(value))
    send_mail.Email().send(['kharesp28@gmail.com'],\
      'Update','run-id:%s'%(value))
    self.start_update_timer()


if __name__=="__main__":
  parser=argparse.ArgumentParser(description='script for monitoring test execution')
  parser.add_argument('-zk_connector',required=True)
  args=parser.parse_args()

  tracker=Track(args.zk_connector)
  tracker.start()
  def signal_handler(signal,frame):
    tracker.stop()
    sys.exit(0)
  signal.signal(signal.SIGINT, signal_handler)
  while True:
    time.sleep(10)
