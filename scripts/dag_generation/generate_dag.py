import graphviz,erdos_renyi,fan_in_fan_out
import numpy as np
import os

output_path='/home/shweta/workspace/research/dag-placement/dags/fan_in_fan_out2'
intermediate_vertices=[8]
number_of_graphs=5

if __name__=="__main__":
  for v in intermediate_vertices:
    print('Generating dags for v:%d'%(v))
    #create sub-directory for a vertex count
    if not os.path.exists('%s/v%d'%(output_path,v)):
      os.makedirs('%s/v%d'%(output_path,v))
    #create sub-directory for adjacency matrices 
    if not os.path.exists('%s/v%d/adj'%(output_path,v)):
      os.makedirs('%s/v%d/adj'%(output_path,v))
    #create sub-directory for dot files 
    if not os.path.exists('%s/v%d/dot'%(output_path,v)):
      os.makedirs('%s/v%d/dot'%(output_path,v))
    #create sub-directory for graphviz plots 
    if not os.path.exists('%s/v%d/img'%(output_path,v)):
      os.makedirs('%s/v%d/img'%(output_path,v))

    graphs=[]
    for i in range(number_of_graphs):
      while True:
        adj=fan_in_fan_out.fan_in_fan_out(v+2,2,2)
        equality=[np.array_equal(adj,g) for g in graphs]
        if not any(equality): 
          graphs.append(adj)
          break
     
      np.savetxt('%s/v%d/adj/v%d_g%d.txt'%(output_path,v,v,i+1), adj.astype(int), fmt='%i', delimiter=",")
      
      graphviz.generate_graphviz_code(adj,\
        '%s/v%d/dot/v%d_g%d.dot'%(output_path,v,v,i+1),\
        descending=True)
      graphviz.visualize_graphviz_code(\
        '%s/v%d/dot/v%d_g%d.dot'%(output_path,v,v,i+1),\
        '%s/v%d/img/v%d_g%d.svg'%(output_path,v,v,i+1),\
        'svg')
      graphviz.convert_svg_to_png('%s/v%d/img/v%d_g%d.svg'%(output_path,v,v,i+1),\
        '%s/v%d/img/v%d_g%d.png'%(output_path,v,v,i+1))
