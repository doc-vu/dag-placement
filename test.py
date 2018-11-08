import os,sys
sys.path.insert(0, './scripts/benchmarking/execute')
sys.path.insert(1,'./scripts/summarize')
import distributed,latency,util

remote_log_dir='/home/riaps/workspace/dag-placement/log'

#variables
vertices=2
runs=[1,2]
processing_intervals=[1,5,10,20]
publication_rates=[1,5,10,20,40]
execution_interval=300
local_log_dir='/home/shweta/workspace/research/dag-placement/log/fib'
plots_dir='/home/shweta/workspace/research/dag-placement/plots/fib/data'
dag_file='dags/config/input_rate/fan_in_fan_out/v2/g1/v_2_g_1_c1.txt'
graph_id='v_2_g_1_c1'

#ensure local_log_dir and plots_dir exists
if not os.path.exists(local_log_dir):
  os.makedirs(local_log_dir)

#ensure plots dir exists
if not os.path.exists(plots_dir):
  os.makedirs(plots_dir)

#run experiments
for runid in runs:
  for proc in processing_intervals:
    for rate in publication_rates:
      log_dir='%s/run%d/p%d/r%d'%(local_log_dir,runid,proc,rate)
      if not os.path.exists(log_dir):
        os.makedirs(log_dir)
      distributed.execute(dag_file,\
        graph_id,rate,execution_interval,proc,remote_log_dir,log_dir)

#summarize results
for runid in runs:
  for proc in processing_intervals:
    for rate in publication_rates:
      print('%s/run%d/p%d/r%d/%s/dag'%(local_log_dir,runid,proc,rate,graph_id))
      #util.process('%s/run%d/p%d/r%d/%s/dag'%(local_log_dir,runid,proc,rate,graph_id),
      #  '%s/run%d/p%d/r%d/%s/summary'%(local_log_dir,runid,proc,rate,graph_id))
      latency.process('%s/run%d/p%d/r%d/%s/dag'%(local_log_dir,runid,proc,rate,graph_id),
        '%s/run%d/p%d/r%d/%s/summary'%(local_log_dir,runid,proc,rate,graph_id))

#collate results
bbb=[]
with open('inventory/nodes','r') as f:
  for line in f:
    bbb.append(line.split(' ')[0])

if vertices>len(bbb):
    experiment_node_count=len(bbb)
else:
    experiment_node_count=vertices
  
for runid in runs:
  if not os.path.exists('%s/run%d'%(plots_dir,runid)):
    os.makedirs('%s/run%d'%(plots_dir,runid))

  with open('%s/run%d/latency_avg.csv'%(plots_dir,runid),'w') as f:
    f.write(','.join(['p%d'%(i) for i in processing_intervals])+'\n')
    for rate in publication_rates:
      val_str=''
      for proc in processing_intervals:
        with open('%s/run%d/p%d/r%d/%s/summary/summary_latency.csv'%(local_log_dir,runid,proc,rate,graph_id),'r') as inp:
          next(inp)
          val_str+=next(inp).strip().split(',')[1]+','
      f.write(val_str.strip(',')+'\n')
  
  with open('%s/run%d/latency_90th.csv'%(plots_dir,runid),'w') as f:
    f.write(','.join(['p%d'%(i) for i in processing_intervals])+'\n')
    for rate in publication_rates:
      val_str=''
      for proc in processing_intervals:
        with open('%s/run%d/p%d/r%d/%s/summary/summary_latency.csv'%(local_log_dir,runid,proc,rate,graph_id),'r') as inp:
          next(inp)
          val_str+=next(inp).strip().split(',')[2]+','
      f.write(val_str.strip(',')+'\n')


  for node in bbb[0:experiment_node_count]:
    with open('%s/run%d/cpu_%s.csv'%(plots_dir,runid,node),'w') as f:
      f.write(','.join(['p%d'%(i) for i in processing_intervals])+'\n')
      for rate in publication_rates:
        val_str=''
        for proc in processing_intervals:
          with open('%s/run%d/p%d/r%d/%s/summary/summary_util.csv'%(local_log_dir,runid,proc,rate,graph_id),'r') as inp:
            for line in inp:
              if line.startswith(node):
                val_str+=line.strip().split(',')[1]+','
        f.write(val_str.strip(',')+'\n')
  
  for node in bbb[0:experiment_node_count]:
    with open('%s/run%d/mem_%s.csv'%(plots_dir,runid,node),'w') as f:
      f.write(','.join(['p%d'%(i) for i in processing_intervals])+'\n')
      for rate in publication_rates:
        val_str=''
        for proc in processing_intervals:
          with open('%s/run%d/p%d/r%d/%s/summary/summary_util.csv'%(local_log_dir,runid,proc,rate,graph_id),'r') as inp:
            for line in inp:
              if line.startswith(node):
                val_str+=line.strip().split(',')[3]+','
        f.write(val_str.strip(',')+'\n')
  
  for node in bbb[0:experiment_node_count]:
    with open('%s/run%d/nw_%s.csv'%(plots_dir,runid,node),'w') as f:
      f.write(','.join(['p%d'%(i) for i in processing_intervals])+'\n')
      for rate in publication_rates:
        val_str=''
        for proc in processing_intervals:
          with open('%s/run%d/p%d/r%d/%s/summary/summary_util.csv'%(local_log_dir,runid,proc,rate,graph_id),'r') as inp:
            for line in inp:
              if line.startswith(node):
                val_str+=line.strip().split(',')[4]+','
        f.write(val_str.strip(',')+'\n')
