import subprocess
import numpy as np

cpu_methods="ackermann bitops callfunc cdouble cfloat clongdouble correlate crc16 decimal32 decimal64 decimal128 dither djb2a double euler explog fft fibonacci float fnv1a gamma gcd gray hamming hanoi hyperbolic idct int128 int64 int32 int16 int8 int128float int128double int128longdouble int128decimal32 int128decimal64 int128decimal128 int64float int64double int64longdouble int32float int32double int32longdouble jenkin jmp ln2 longdouble loop matrixprod nsqrt omega parity phi pi pjw prime psi queens rand rand48 rgb sdbm sieve sqrt trig union zeta"

if __name__=="__main__":
  with open('stress-ng.csv','w') as f:
    f.write('method,run1,run2,run3,run4,run5,avg,std_dev,ms_per_op\n')
    for method in cpu_methods.split(" "):
      print('Testing method:%s'%(method))
      res=[]
      for run in range(5):
        print('Running test:%d'%(run+1))
        ret=subprocess.check_output(['stress-ng','--cpu','1','--cpu-method','sqrt','-t','10s','--metrics-brief'])
        ops_per_sec=ret.split('\n')[4].split()[8]
        res.append(float(ops_per_sec))
      f.write('%s,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f\n'%(method,\
        res[0],\
        res[1],\
        res[2],\
        res[3],\
        res[4],\
        np.mean(res),\
        np.std(res),
        1000/np.mean(res)))
