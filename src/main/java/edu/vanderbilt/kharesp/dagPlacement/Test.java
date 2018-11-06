package edu.vanderbilt.kharesp.dagPlacement;


public class Test {
	public static void main(String args[]){
		if (args.length<1){
			System.out.println("Usage: Test t");
			return;
		}
		int t=Integer.parseInt(args[0]);
		for(int i=0;i<10000;i++){
			long startTs = System.currentTimeMillis();
			for(int j=0;j<t;j++){
				fib(22);
			}
			long endTs = System.currentTimeMillis();
			System.out.format("%d,%d,%d\n",i+1, t, (endTs - startTs));
		}
	}

	public static int fib(int n) {
		if (n <= 1)
			return n;
		return fib(n - 1) + fib(n - 2);
	}

}
