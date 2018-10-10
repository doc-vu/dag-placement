def combinations(selectivity,vertices):
  patterns=set()
  def compute(prefix,length):
    if length==0:
      patterns.add(prefix)
    else:
      for item in selectivity:
        if len(prefix)>0:
          new_prefix=prefix+','+item
        else:
          new_prefix=item
        compute(new_prefix,length-1)
  compute('',vertices)
  return patterns

if __name__=="__main__":
  for vcount in range(4,9):
    patterns=combinations(['.5','1'],vcount)
    with open('dags/config/selectivity/v%d'%(vcount),'w') as f:
      for p in patterns:
        f.write(p+'\n')
