package edu.vanderbilt.kharesp.zmq.dagPlacement;

import org.zeromq.ZMQ;

import edu.vanderbilt.kharesp.zmq.types.DataSampleHelper;
public class ZMQPublisher {

	public static void main(String args[]){
		if(args.length<2){
			System.out.println("usage: ZMQPublisher publicationRate executionInterval");
			return;
		}
		try{
			int publicationRate = Integer.parseInt(args[0]);
			int executionInterval = Integer.parseInt(args[1]);
			ZMQ.Context context = ZMQ.context(1);
			ZMQ.Socket pub = context.socket(ZMQ.PUB);
			pub.bind("tcp://*:5000");
			Thread.sleep(10000);
		
			DataSampleHelper fb=new DataSampleHelper();
			int sleep_interval=1000/publicationRate;
			for(int i=0;i<publicationRate*executionInterval;i++){
				pub.sendMore("test".getBytes());
				pub.send(fb.serialize(i+1, // sampleId
					 System.currentTimeMillis(), // sourceTs 
					 52)); // payloadSize
				Thread.sleep(sleep_interval);
				if(i%100==0){
					System.out.println(i);
				}
			}
			pub.setLinger(0);
			pub.close();
			context.close();

		}catch(Exception e){
			e.printStackTrace();
		}

	}
}
