import os
import pandas as pd
k_colocation=5
log_dir='log/model_learning_urd_2_no_batch/data/k%d'%(k_colocation)
experiments=os.listdir(log_dir)

for experiment_id in [int(x) for x in experiments]:
  for k in range(1,k_colocation+1):
    with open('%s/%d/graphs/g%d.txt'%(log_dir,experiment_id,k),'r') as ig:
      next(ig)#skip header   
      for line in ig:
        if 'g%d_v1'%(k) in line:
          node,vertex,subscription,publication,\
          selectivity,input_rate,sinks,sources,vertices,\
          publication_rate,processing_interval= line.rstrip().split(';')
    data=pd.read_csv('%s/%d/data/g%d/dag/g%d_e12.csv'%(log_dir,experiment_id,k,k),\
      names=['edge','sample_id','latency'],delimiter=',')
    expected_number_of_samples=int(int(publication_rate)*float(selectivity)*120)
    if float(selectivity)==.5:
      seq=data['sample_id'].diff().dropna().eq(2).all()
    else:
      seq=data['sample_id'].diff().dropna().eq(1).all()
    if (len(data)!=expected_number_of_samples) or (not seq):
      print('Data for experiment:%d are invalid'%(experiment_id))
