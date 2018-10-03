import graphviz,erdos_renyi,fan_in_fan_out
import numpy as np
import os

output_path='/home/kharesp/workspace/python/dag-placement/dags/fan_in_fan_out'
vertices=[i for i in range(4,9)]
number_of_graphs=5

if __name__=="__main__":
  for v in vertices:
    #print('Generating dags for v:%d'%(v))
    ##create sub-directory for a vertex count
    #if not os.path.exists('%s/v%d'%(output_path,v)):
    #  os.makedirs('%s/v%d'%(output_path,v))
    ##create sub-directory for adjacency matrices 
    #if not os.path.exists('%s/v%d/adj'%(output_path,v)):
    #  os.makedirs('%s/v%d/adj'%(output_path,v))
    ##create sub-directory for dot files 
    #if not os.path.exists('%s/v%d/dot'%(output_path,v)):
    #  os.makedirs('%s/v%d/dot'%(output_path,v))
    ##create sub-directory for graphviz plots 
    #if not os.path.exists('%s/v%d/img'%(output_path,v)):
    #  os.makedirs('%s/v%d/img'%(output_path,v))

    #graphs=[]
    for i in range(number_of_graphs):
      #while True:
      #  adj=fan_in_fan_out.fan_in_fan_out(v,2,2)
      #  equality=[np.array_equal(adj,g) for g in graphs]
      #  if not any(equality): 
      #    graphs.append(adj)
      #    print(len(graphs))
      #    break
     
      #np.savetxt('%s/v%d/adj/v_%d_g_%d.txt'%(output_path,v,v,i+1), adj.astype(int), fmt='%i', delimiter=",")
      #
      #graphviz.generate_graphviz_code(adj,\
      #  '%s/v%d/dot/v_%d_g_%d.dot'%(output_path,v,v,i+1),\
      #  descending=True)
      graphviz.visualize_graphviz_code(\
        '%s/v%d/dot/v_%d_g_%d.dot'%(output_path,v,v,i+1),\
        '%s/v%d/img/v_%d_g_%d.svg'%(output_path,v,v,i+1),\
        'svg')
      graphviz.convert_svg_to_png('%s/v%d/img/v_%d_g_%d.svg'%(output_path,v,v,i+1),\
        '%s/v%d/img/v_%d_g_%d.png'%(output_path,v,v,i+1))
