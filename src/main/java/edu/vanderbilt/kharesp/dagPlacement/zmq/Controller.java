package edu.vanderbilt.kharesp.dagPlacement.zmq;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.zeromq.ZMQ;
import edu.vanderbilt.kharesp.dagPlacement.util.Util;

public class Controller {
	
	public static void main(String args[]){
		try {
			//start connection to ZK
			CuratorFramework client = CuratorFrameworkFactory.newClient(Util.ZK_CONNECTOR,
					new ExponentialBackoffRetry(1000, 3));
			client.start();
			DistributedBarrier startBarrier = new DistributedBarrier(client, "/barriers/start");
			DistributedBarrier endBarrier = new DistributedBarrier(client, "/barriers/end");
			DistributedBarrier exitBarrier = new DistributedBarrier(client, "/barriers/exit");

			//create control command publisher 
			ZMQ.Context context = ZMQ.context(1);
			ZMQ.Socket pub = context.socket(ZMQ.PUB);
			pub.setHWM(0);
			pub.bind("tcp://*:5000");

			//wait for all DAGs to initialize
			startBarrier.wait();
			pub.send(String.format("%s %s", Util.CONTROL_TOPIC, Util.CTRL_CMD_START_PUBLISHING));
			//wait for all DAGs to finish execution
			endBarrier.wait();
			pub.send(String.format("%s %s", Util.CONTROL_TOPIC, Util.CTRL_CMD_EXIT));
			//exit after all vertices have exited
			exitBarrier.wait();

			// cleanup
			client.close();
			pub.setLinger(0);
			pub.close();
			context.close();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
