package edu.vanderbilt.kharesp.zmq.dagPlacement;

import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import edu.vanderbilt.kharesp.dagPlacement.Util;
import edu.vanderbilt.kharesp.zmq.types.DataSample;
import edu.vanderbilt.kharesp.zmq.types.DataSampleHelper;

public class ZMQSubscriber {
	public static void main(String args[]){
		if(args.length<3){
			System.out.println("Usage: ZMQSubscriber publicationRate executionInterval processingInterval");
			return;
		}
		try{
			int publicationRate = Integer.parseInt(args[0]);
			int executionInterval = Integer.parseInt(args[1]);
			int processingInterval= Integer.parseInt(args[2]);

			ZMQ.Context context = ZMQ.context(1);
			ZMQ.Socket sub = context.socket(ZMQ.SUB);
			sub.setHWM(0);
			sub.connect("tcp://172.21.20.49:5000");
			sub.subscribe("test".getBytes());
			
			int receiveCount=0;
			while(receiveCount<publicationRate*executionInterval){
				ZMsg msg = ZMsg.recvMsg(sub);
				DataSample sample = DataSampleHelper.deserialize(msg.getLast().getData());
				long sourceTs=sample.sourceTs();

				if (Util.bogusIterations.containsKey(processingInterval)){
					for(int i=0; i< Util.bogusIterations.get(processingInterval);i++){
						Util.fib(22);
					}
				}
				long receiveTs = System.currentTimeMillis();
				System.out.println(String.format("%d,%d", sample.sampleId(), receiveTs - sourceTs));
				receiveCount++;
			}
			
			sub.setLinger(0);
			sub.close();
			context.close();

		}catch(Exception e){
			e.printStackTrace();
		}
	}
}
