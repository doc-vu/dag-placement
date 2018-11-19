import os,sys
sys.path.insert(0, './scripts/benchmarking/execute')
import experiment

def test_colocation(k,count,log_dir):
  with open('log/model_learning/parameters/k%d'%(k),'r') as inp:
    next(inp) #skip header
    for i in range(count):
      #make sub-dirs
      if not os.path.exists('%s/%d'%(log_dir,i+1)):
        os.makedirs('%s/%d'%(log_dir,i+1))
      if not os.path.exists('%s/%d/graphs'%(log_dir,i+1)):
        os.makedirs('%s/%d/graphs'%(log_dir,i+1))

      parameters=next(inp).strip()
      dags=parameters.split(',')
      for idx,dag in enumerate(dags):
        with open('%s/%d/graphs/g%d.txt'%(log_dir,i+1,idx+1),'w') as f:
          processing_interval,publication_rate=dag.split(':')
          f.write('node;vertex;subscription;publication;selectivity;input_rate;sinks;sources;vertices;publication_rate,processing_interval\n')
          f.write('localhost;g%d_v0;;g%d_e01;%f;%f;1;1;3;%s;-1\n'%(idx+1,idx+1,1,1,publication_rate))
          f.write('bbb-6302;g%d_v1;g%d_e01;g%d_e12;%f;%f;1;1;3;%s;%s\n'%(idx+1,idx+1,idx+1,1,1,publication_rate,processing_interval))
          f.write('bbb-7d6a;g%d_v2;g%d_e12;;%f;%f;1;1;3;%s;-1\n'%(idx+1,idx+1,1,1,publication_rate))

      #execute experiment
      experiment.Experiment('129.59.105.159:2181',\
        ''%(), #config_dir
        '',\ #remote_log_dir
        ''%(), #local_log_dir
        ).run() #execution_time
                #zmq
          

if __name__=="__main__":
  test_colocation(4,2,'log/model_learning/k4')
