import random
import numpy as np
processing_intervals=[1,5,10,15,20]
max_publication_rate=20
max_vertices=12


def create_random_parameters(k,count):
  if k==1:
    max_foreground_len=12
  else:
    max_foreground_len=6

  combinations=set()
  while (len(combinations)<count):
    count_vertices=0
    sum_proc=0
    params={}
    #select length of foreground linear chain
    fv=random.randint(1,max_foreground_len)
    count_vertices+=fv
    fp=[processing_intervals[random.randint(1,len(processing_intervals)-1)] for i in range(fv)]
    sum_proc+=sum(fp)
    
    params[1]=fp
    for background_chain in range(2,k+1):
      if (count_vertices==max_vertices):
        break
      #select length of background linear chain
      bv=random.randint(1,max_vertices-count_vertices)
      count_vertices+=bv
      bp=[processing_intervals[random.randint(1,len(processing_intervals)-1)] for i in range(bv)]
      sum_proc+=sum(bp)
      params[background_chain]=bp

    if len(params)==k:
      msr=1000//sum_proc
      #print(msr)
      if msr>0:
        if msr>max_publication_rate:
          msr=max_publication_rate
        parameter_str=''
        for background_chain in range(1,k+1):
          parameter_str+='%d;%d;%s/'%(len(params[background_chain]),random.randint(1,msr),','.join([str(p) for p in params[background_chain]]))
        combinations.add(parameter_str.strip('/'))
  return combinations 


for k in range(1,7):
  with open('log/model_learning_linear/parameters/k%d'%(k),'w') as f:
    for c in (create_random_parameters(k,2000)):
      f.write(c+'\n')
