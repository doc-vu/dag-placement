import os,sys
sys.path.insert(0, './scripts/benchmarking/execute')
sys.path.insert(1,'./scripts/summarize')
import distributed,latency,util

remote_log_dir='/home/riaps/workspace/dag-placement/log'

#variables
vertices=2
runs=[1,2,3]
processing_intervals=[-1,1,5,10,20]
publication_rates=[1,5,10,20,40]
execution_interval=300
local_log_dir='/home/shweta/workspace/research/dag-placement/log/tight-loop'
plots_dir='/home/shweta/workspace/research/dag-placement/plots/tight-loop/data'

#run experiments
for runid in runs:
  for proc in processing_intervals:
    for rate in publication_rates:
      log_dir='%s/run%d/p%d/r%d'%(local_log_dir,runid,proc,rate)
      if not os.path.exists(log_dir):
        os.makedirs(log_dir)
      distributed.execute('dags/config/input_rate/fan_in_fan_out/v2/g1/v_2_g_1_c1.txt',\
        'v_2_g_1_c1',rate,execution_interval,proc,remote_log_dir,log_dir)

#summarize results
for runid in runs:
  for proc in processing_intervals:
    for rate in publication_rates:
      util.process('%s/run%d/p%d/r%d/v_2_g_1_c1/dag'%(local_log_dir,runid,proc,rate),
        '%s/run%d/p%d/r%d/v_2_g_1_c1/summary'%(local_log_dir,runid,proc,rate))
      latency.process('%s/run%d/p%d/r%d/v_2_g_1_c1/dag'%(local_log_dir,runid,proc,rate),
        '%s/run%d/p%d/r%d/v_2_g_1_c1/summary'%(local_log_dir,runid,proc,rate))

#collate results
bbb=[]
with open('inventory/nodes','r') as f:
  for line in f:
    bbb.append(line.split(' ')[0])
  
for runid in runs:
  if not os.path.exists('%s/run%d'%(plots_dir,runid)):
    os.makedirs('%s/run%d'%(plots_dir,runid))

  with open('%s/run%d/latency_avg.csv'%(plots_dir,runid),'w') as f:
    f.write('p-1,p1,p5,p10,p20\n')
    for rate in [1,5,10,20,40]:
      val_str=''
      for proc in [-1,1,5,10,20]:
        with open('%s/run%d/p%d/r%d/v_2_g_1_c1/summary/summary_latency.csv'%(local_log_dir,runid,proc,rate),'r') as inp:
          next(inp)
          val_str+=next(inp).strip().split(',')[1]+','
      f.write(val_str.strip(',')+'\n')
  
  with open('%s/run%d/latency_90th.csv'%(plots_dir,runid),'w') as f:
    f.write('p-1,p1,p5,p10,p20\n')
    for rate in [1,5,10,20,40]:
      val_str=''
      for proc in [-1,1,5,10,20]:
        with open('%s/run%d/p%d/r%d/v_2_g_1_c1/summary/summary_latency.csv'%(local_log_dir,runid,proc,rate),'r') as inp:
          next(inp)
          val_str+=next(inp).strip().split(',')[2]+','
      f.write(val_str.strip(',')+'\n')


  for node in bbb[0:vertices]:
    print(node)
    with open('%s/run%d/cpu_%s.csv'%(plots_dir,runid,node),'w') as f:
      f.write('p-1,p1,p5,p10,p20\n')
      for rate in [1,5,10,20,40]:
        val_str=''
        for proc in [-1,1,5,10,20]:
          with open('%s/run%d/p%d/r%d/v_2_g_1_c1/summary/summary_util.csv'%(local_log_dir,runid,proc,rate),'r') as inp:
            for line in inp:
              if line.startswith(node):
                val_str+=line.strip().split(',')[1]+','
        f.write(val_str.strip(',')+'\n')
  
  for node in bbb[0:vertices]:
    with open('%s/run%d/mem_%s.csv'%(plots_dir,runid,node),'w') as f:
      f.write('p-1,p1,p5,p10,p20\n')
      for rate in [1,5,10,20,40]:
        val_str=''
        for proc in [-1,1,5,10,20]:
          with open('%s/run%d/p%d/r%d/v_2_g_1_c1/summary/summary_util.csv'%(local_log_dir,runid,proc,rate),'r') as inp:
            for line in inp:
              if line.startswith(node):
                val_str+=line.strip().split(',')[3]+','
        f.write(val_str.strip(',')+'\n')
  
  for node in bbb[0:vertices]:
    with open('%s/run%d/nw_%s.csv'%(plots_dir,runid,node),'w') as f:
      f.write('p-1,p1,p5,p10,p20\n')
      for rate in [1,5,10,20,40]:
        val_str=''
        for proc in [-1,1,5,10,20]:
          with open('%s/run%d/p%d/r%d/v_2_g_1_c1/summary/summary_util.csv'%(local_log_dir,runid,proc,rate),'r') as inp:
            for line in inp:
              if line.startswith(node):
                val_str+=line.strip().split(',')[4]+','
        f.write(val_str.strip(',')+'\n')
