import argparse,random

def configurations(colocation,count,output_file):
  processing_intervals=[1,5,10,20]
  publication_rates=[1,5,10,20,25,40]
  combinations=set()
  while len(combinations)<count:
    combination=''
    for k in range(colocation):
      proc=processing_intervals[random.randint(0,len(processing_intervals)-1)]
      rate=publication_rates[random.randint(0,len(publication_rates)-1)]
      combination=combination + '%d:%d,'%(proc,rate)
    combinations.add(combination.rstrip(','))
  
  with open(output_file,'w') as f:
    f.write(','.join(['p%d:r%d'%(i+1,i+1) for i in range(colocation)])+'\n')
    for pattern in combinations:
      f.write(pattern +'\n')

      

if __name__=="__main__":
  parser=argparse.ArgumentParser(description='script for creating dataset for k-vertex colocation model learning')
  parser.add_argument('-k',help='degree of colocation of vertices',required=True,type=int)
  parser.add_argument('-count',help='number of test parameterizations to create',required=True,type=int)
  parser.add_argument('-output_file',help='output file path',required=True)
  args=parser.parse_args()

  configurations(args.k,args.count,args.output_file)
