import sys,os,shutil
sys.path.insert(0, './scripts/benchmarking/execute')
import guarded
import numpy as np


domain_id=1
zk_connector='129.59.105.159:2181'

ineligible_dags=['v5_g3.txt','v7_g5.txt','v8_g1.txt','v8_g2.txt']
dag_dir='/home/shweta/workspace/research/dag-placement/dags/fan_in_fan_out2'
log_dir='/home/shweta/workspace/research/dag-placement/log/greedy_pmax_20_rmax_15_nc_10'


bbb_mapping={
  'bbb1':'bbb-a702',
  'bbb2':'bbb-ed97',
  'bbb3':'bbb-bce4',
  'bbb4':'bbb-bcf6',
  'bbb5':'bbb-95f9',
  'bbb6':'bbb-8014',
  'bbb7':'bbb-6302',
  'bbb8': 'bbb-c473',
}

def check_similarity(placements,given_placement,vertex_count):
  if len(placements)==0:
    return False
  res=[]
  for curr_placement in placements.values(): 
    print(curr_placement)
    similar=True
    for idx in range(1,vertex_count-1):
      if curr_placement['%d'%(idx)]!=given_placement['%d'%(idx)]:
        similar=False
        break
    res.append(similar)
  return any(res) 
    
for v in range(2,6):
  graphs=os.listdir('%s/v%d/adj'%(dag_dir,v))
  for gid in range(1,len(graphs)+1):
    if 'v%d_g%d.txt'%(v,gid) in ineligible_dags:
      continue
    for tid in range(1,2):
      placements={}
      for method in ['lpp','sum','const','naive-1']:
        print('\n\n\n\nEXECUTING placement for v:%d,gid:%d,method:%s,tid:%d,log_dir:%s'\
          %(v,gid,method,tid,log_dir))

        adj=np.loadtxt('%s/v%d/adj/v%d_g%d.txt'%(dag_dir,v,v,gid),dtype=int,delimiter=',')
        vertex_count=np.shape(adj)[0]
        sources=list(np.where(~adj.any(axis=0))[0])
        sinks=list(np.where(~adj.any(axis=1))[0])

        #load parameters
        with open('%s/v%d/g%d/%d/%s/heuristic/params.csv'%(log_dir,v,gid,tid,method),'r') as f:
          next(f)#skip header
          rate,p=next(f).strip().split(';')
        
        processing_intervals=p.split(',')

        #load placement
        placement={}
        with open('%s/v%d/g%d/%d/%s/heuristic/placement.csv'%(log_dir,v,gid,tid,method),'r') as f:
          next(f)#skip header
          for line in f:
            node,vertices=line.strip().split(';')
            for x in vertices.split(','):
              placement[x]=node

        #write graph description
        with open('%s/v%d/g%d/%d/%s/graphs/g1.txt'%(log_dir,v,gid,tid,method),'w') as f:
          f.write('node;vertex;subscription;publication;selectivity;input_rate;sinks;sources;vertices;publication_rate;processing_interval\n')
          for vid in range(vertex_count):
            subscriptions= np.nonzero(adj[:,vid])[0]
            publications=np.nonzero(adj[vid,:])[0]
            #source
            if len(subscriptions)==0:
              f.write('localhost;g1_v%d;;%s;1;1;%d;%d;%d;%s;-1\n'%(vid,','.join(['g1_e%d%d'%(vid,x) for x in publications]),len(sinks),len(sources),vertex_count,rate))
            #sink
            elif len(publications)==0:
              f.write('localhost;g1_v%d;%s;;1;%d;%d;%d;%d;%s;-1\n'%(vid,','.join(['g1_e%d%d'%(x,vid) for x in subscriptions]),len(subscriptions),len(sinks),len(sources),vertex_count,rate))
            else:
              f.write('%s;g1_v%d;%s;%s;1;%d;%d;%d;%d;%s;%s\n'%(bbb_mapping[placement['%d'%(vid)]],vid,\
                ','.join(['g1_e%d%d'%(x,vid) for x in subscriptions]),','.join(['g1_e%d%d'%(vid,x) for x in publications]),
                len(subscriptions),len(sinks),len(sources),vertex_count,rate,processing_intervals[vid-1]))

        #check similarity of placements
        similar=check_similarity(placements,placement,vertex_count)
        placements[method]=placement
        if not similar:
          print('Executing placement:%s'%(method))
          config_dir='%s/v%d/g%d/%d/%s'%(log_dir,v,gid,tid,method)
          #execute experiment
          guarded.Guarded(zk_connector=zk_connector,\
            config_dir=config_dir,\
            remote_log_dir='/home/riaps/workspace/dag-placement/log/%d'%(domain_id),\
            local_log_dir=config_dir,\
            execution_time=120,domain_id=domain_id).run()
