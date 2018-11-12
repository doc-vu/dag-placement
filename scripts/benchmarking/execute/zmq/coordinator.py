from kazoo.client import KazooClient
from kazoo.recipe.watchers import ChildrenWatch
from kazoo.protocol.states import EventType
import zmq,argparse,time
import netifaces as ni

class Coordinate(object):
  def __init__(self,zk_connector,graph_id,vertex_count,sink_count):
    self.graph_id=graph_id
    self.vertex_count=vertex_count
    self.sink_count=sink_count

    self._host_ip=ni.ifaddresses('eth0')[ni.AF_INET][0]['addr']
    self._context=zmq.Context()
    self._socket=self._context.socket(zmq.PUB)
    self._socket.set_hwm(0)
    self._bind_port=self._socket.bind_to_random_port('tcp://*')

    self._zk=KazooClient(hosts=zk_connector)
    self._zk.start()
    self._exited=False

  def clean(self):
    if self._zk.exists('/%s'%(self.graph_id)):
      self._zk.delete('/%s'%(self.graph_id),recursive=True)

  def create_paths(self):
    self._zk.ensure_path('/%s'%(self.graph_id))
    self._zk.create('/%s/control'%(self.graph_id),('tcp://%s:%d'%(self._host_ip,self._bind_port)).encode())
    self._zk.ensure_path('/%s/topics'%(self.graph_id))
    self._zk.ensure_path('/%s/coord/joined'%(self.graph_id))
    self._zk.ensure_path('/%s/coord/sink'%(self.graph_id))

  def install_watches(self):
    def _joined_endpoint_listener(children,event):
      if event and event.type==EventType.CHILD:
        if 'joined' in event.path:
          if (len(children)==self.vertex_count):
            print('All vertices have joined.Will send start command')
            #send start command
            self._socket.send_string('%s start'%(self.graph_id))
            return False
        elif 'sink' in event.path:
          if (len(children)==self.sink_count):
            print('All sinks have received data.Will send exit command')
            self._exited=True
            #send exit command
            self._socket.send_string('%s exit'%(self.graph_id))
            return False

    ChildrenWatch(client=self._zk,\
      path='%s/coord/joined'%(self.graph_id),\
      func=_joined_endpoint_listener,send_event=True)

    ChildrenWatch(client=self._zk,\
      path='%s/coord/sink'%(self.graph_id),\
      func=_joined_endpoint_listener,send_event=True)

  def stop(self):
    self._zk.stop()
    self._socket.setsockopt(zmq.LINGER,0)
    self._socket.close()
    self._context.close()

  def run(self):
    self.clean()
    self.create_paths()
    self.install_watches()

    #wait until DAG finishes execution
    while(not self._exited):
      time.sleep(5)

    self.stop()

if __name__=="__main__":
  parser=argparse.ArgumentParser(description='script to coordinate the execution of a DAG')
  parser.add_argument('-zk_connector',help='zk connector',required=True)
  parser.add_argument('-graph_id',help='DAG id',required=True)
  parser.add_argument('-vertices',type=int,help='number of vertices',required=True)
  parser.add_argument('-sinks',type=int,help='number of sinks',required=True)
  args=parser.parse_args()
  
  Coordinate(zk_connector=args.zk_connector,graph_id=args.graph_id,
    vertex_count=args.vertices,sink_count=args.sinks).run()
