package edu.vanderbilt.kharesp.dagPlacement.util;

import java.util.concurrent.TimeUnit;

public class TestPublicationRateSleep {

	public static void test(int pubRate){
		int count=0;
		long start_time=getTimeUSec();
		long last_time_check=getTimeUSec();
		long time_now=0,time_delta=0;

		long sleepNanosec=1000000000/pubRate;
		long sleepUsec=1000;

		double rate=0;
		double runningSum=0;
		long runningSumCounter=0;
	
		long directSleepInterval=0;
		boolean useDirectSleepInterval=false;

		if(1000%pubRate==0){
			useDirectSleepInterval=true;
			directSleepInterval=1000000000/pubRate;
		}
		while(true){
			if (count > 0) {
				time_now = getTimeUSec();
				if ((time_now - start_time) >= 120000000) {
					System.out.format("%d,%f\n", pubRate,
							(runningSum * 1.0) / runningSumCounter);
					break;
				}

				time_delta = time_now - last_time_check;
				last_time_check = time_now;
				rate = 1000000.0 / time_delta;
				if(count>=2){
					runningSum += rate;
					runningSumCounter+=1;
				}

				if (rate > pubRate) {
					sleepNanosec += sleepUsec; // plus 1 MicroSec
				} else if (rate < pubRate && sleepNanosec > sleepUsec) {
					sleepNanosec -= sleepUsec; // less 1 MicroSec
				} else if (rate < pubRate && sleepNanosec <= sleepUsec) {
					sleepNanosec = 0;
				}
				try{
					if (!useDirectSleepInterval) {
						if (sleepNanosec > 0) {
							TimeUnit.NANOSECONDS.sleep(sleepNanosec);
						}
					} else {
						TimeUnit.NANOSECONDS.sleep(directSleepInterval);
					}
				}catch(Exception e){
					e.printStackTrace();
				}
			}
			count += 1;
		}
	}

	public static void main(String args[]){
		for(int pubRate=1;pubRate<=100;pubRate++){
			test(pubRate);
		}
	}

	public static long getTimeUSec(){
		return System.nanoTime()/1000;
	}
}