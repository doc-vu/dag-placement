import subprocess
import numpy as np

def generate_graphviz_code(adj,output,descending=False):
  graphviz_formatting='digraph{\n\
  rankdir=LR\n\
  node [shape=circle,fixedsize=true,width=.26]\n\
  edge [penwidth=0.75,arrowsize=0.5]\n\
'
  with open(output,'w') as f:
    f.write(graphviz_formatting)
    if descending:
      vertices=[i for i in range(adj.shape[0]-1,-1,-1)]
    else:
      vertices=[i for i in range(adj.shape[0])]
    for i in vertices:
      f.write('  %d -> {%s}\n'%(i,' '.join([str(i) for i in np.nonzero(adj[i])[0]])))

    f.write('}\n')

def visualize_graphviz_code(input_file,output_file,image_format):
  subprocess.check_call(['dot','-T%s'%(image_format),input_file,'-o',output_file])  

def convert_svg_to_png(input_svg,output_png):
  subprocess.check_call(['inkscape',input_svg,'-e', output_png])
