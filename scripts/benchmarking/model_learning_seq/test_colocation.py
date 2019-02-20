import os,sys,time,subprocess,argparse
from kazoo.client import KazooClient
import pandas as pd
sys.path.insert(0, './scripts/benchmarking/execute')
import guarded

def time_offset_check(nodes):
  open('/home/shweta/workspace/research/dag-placement/offsets.csv','w').close()
  subprocess.check_call(['ansible-playbook','playbooks/offset.yml',\
    '--limit',','.join(nodes)])
  data=pd.read_csv('/home/shweta/workspace/research/dag-placement/offsets.csv',names=['host','offset'],delimiter=',')
  return (abs(data['offset']) <5).all()

def test_colocation(k,count,start_id,log_dir,zk_connector,domain_id,bbb):
  zk=KazooClient(zk_connector)
  zk.start()
  with open('%s/parameters/k%d'%(log_dir,k),'r') as inp:
    for i in range(start_id-1+count):
      parameters=next(inp).strip()
      if i<(start_id-1):
        continue
      if(i%10==0):
        #test whether time drift has happened
        if not time_offset_check([bbb]):
          print('Clock has drifted by more than 5ms. Aborting...')
          return
      print('Testing configuration:%d'%(i+1))

      if not zk.exists('/dom%d/runid'%(domain_id)):
        zk.ensure_path('/dom%d/runid'%(domain_id))
      zk.set('/dom%d/runid'%(domain_id),b'%d'%(i+1))
      #make sub-dirs
      if not os.path.exists('%s/data/k%d/%d'%(log_dir,k,i+1)):
        os.makedirs('%s/data/k%d/%d'%(log_dir,k,i+1))
      if not os.path.exists('%s/data/k%d/%d/graphs'%(log_dir,k,i+1)):
        os.makedirs('%s/data/k%d/%d/graphs'%(log_dir,k,i+1))

      dags=parameters.split('/')
      for idx,dag in enumerate(dags):
        with open('%s/data/k%d/%d/graphs/g%d.txt'%(log_dir,k,i+1,idx+1),'w') as f:
          vertices,rate,proc=dag.split(';')
          vcount=int(vertices)
          publication_rate=int(rate)
          processing_intervals=[int(p) for p in proc.split(',')]
         
          f.write('node;vertex;subscription;publication;selectivity;input_rate;sinks;sources;vertices;publication_rate;processing_interval\n')
          f.write('localhost;g%d_v0_%d;;g%d_e01_%d;1.000000;1.000000;1;1;%d;%d;-1\n'%(idx+1,i+1,idx+1,i+1,vcount+2,publication_rate))
          f.write('localhost;g%d_v%d_%d;g%d_e%d%d_%d;;1.000000;1.000000;1;1;%d;%d;-1\n'%(idx+1,vcount+1,i+1,idx+1,vcount,vcount+1,i+1,vcount+2,publication_rate))
          for vid in range(1,vcount+1):
            f.write('%s;g%d_v%d_%d;g%d_e%d%d_%d;g%d_e%d%d_%d;1.000000;1.000000;1;1;%d;%d;%d\n'%\
              (bbb,idx+1,vid,i+1,idx+1,vid-1,vid,i+1,idx+1,vid,vid+1,i+1,vcount+2,publication_rate,processing_intervals[vid-1]))
          

      start_ts=time.time()
      #execute experiment
      guarded.Guarded(zk_connector=zk_connector,\
        config_dir='%s/data/k%d/%d'%(log_dir,k,i+1),\
        remote_log_dir='/home/riaps/workspace/dag-placement/log/%d'%(domain_id),\
        local_log_dir='%s/data/k%d/%d'%(log_dir,k,i+1),\
        execution_time=120,domain_id=domain_id).run()
          
      end_ts=time.time()
      print('Test configuration:%d took %.1f min'%(i+1,(end_ts-start_ts)/60.0))
  zk.stop()

if __name__=="__main__":
  parser=argparse.ArgumentParser(description='script to gather data for model learning')
  parser.add_argument('-k',type=int,required=True,help='degree of co-location')
  parser.add_argument('-count',type=int,required=True,help='number of experiments to perform')
  parser.add_argument('-start_id',type=int,required=True,help='start test id')
  parser.add_argument('-log_dir',required=True,help='parent log directory')
  parser.add_argument('-zk_connector',required=True,help='zookeeper connector')
  parser.add_argument('-domain_id',type=int,required=True,help='DDS domain id')
  parser.add_argument('-bbb',required=True,help='name of bbb hosting dags')
  args=parser.parse_args()

  test_colocation(args.k,args.count,args.start_id,\
    args.log_dir,args.zk_connector,args.domain_id,\
    args.bbb)
