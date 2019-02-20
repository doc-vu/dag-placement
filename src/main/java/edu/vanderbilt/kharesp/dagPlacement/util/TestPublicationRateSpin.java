package edu.vanderbilt.kharesp.dagPlacement.util;
import com.rti.ndds.Utility;

public class TestPublicationRateSpin {
	public static void test(int pubRate){
		long spinPerUsec=Utility.get_spin_per_microsecond();
		long spinLoopCount=1000000*spinPerUsec/pubRate;
		long lastTime=getTimeUsec();
		long startTime=getTimeUsec();
		int count=0;
		
		long timeNow=0,timeDelta=0;
		double rate=0,runningSumRate=0;
		int runningCount=0;

		while(true){
			if(count>0){
				timeNow=getTimeUsec();
				if((timeNow-startTime)>120000000){
					System.out.format("%d,%f\n", pubRate,
							(runningSumRate) / runningCount);
					break;
				}
				timeDelta=timeNow-lastTime;
				lastTime=timeNow;
				rate=1000000.0/timeDelta;

				if(count>1){
					runningSumRate+=rate;
					runningCount+=1;
				}

				if(rate>pubRate){
					spinLoopCount+=spinPerUsec;
				}else if (rate<pubRate && spinLoopCount>spinPerUsec){
					spinLoopCount-=spinPerUsec;
				}else if (rate<pubRate && spinLoopCount<=spinPerUsec){
					spinLoopCount=0;
				}
				if(spinLoopCount>0){
					Utility.spin(spinLoopCount);
				}
			}
			count+=1;
		}
		
	}
	
	public static long getTimeUsec(){
		return System.nanoTime()/1000;
	}

	public static void main(String args[]){
		for(int rate=1;rate<=100;rate++){
			test(rate);
		}
	}
}
