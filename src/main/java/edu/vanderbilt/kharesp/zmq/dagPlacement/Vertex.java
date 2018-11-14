package edu.vanderbilt.kharesp.zmq.dagPlacement;

import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map.Entry;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.data.Stat;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;
import edu.vanderbilt.kharesp.dagPlacement.Util;
import edu.vanderbilt.kharesp.zmq.types.DataSample;
import edu.vanderbilt.kharesp.zmq.types.DataSampleHelper;

public class Vertex {
	public static final String ZK_CONNECTOR="129.59.105.159:2181";
	public static final String CTRL_CMD_START_PUBLISHING="start";
	public static final String CTRL_CMD_EXIT="exit";

	private Logger logger;

	private String graphId;
	private String vId;
	private float selectivity;
	private float inputRate;
	private int publicationRate;
	private int executionTime;
	private String logDir;
	private int processingInterval;
	private PrintWriter pw;

	
	private boolean source;
	private boolean sink;
	private boolean cleanupCalled;
	
	private CuratorFramework client;

	private String ipAddr;
	private ZMQ.Context context;
	private ZMQ.Poller poller;
	private HashMap<String,ZMQ.Socket> publisherSockets; 
	private ZMQ.Socket subscriberSocket;
	private ZMQ.Socket controlSocket;

	public Vertex(String graphId,String vId,
			ArrayList<String> incomingEdges,ArrayList<String> outgoingEdges,
			float selectivity,float inputRate,
			int sinkCount,int sourceCount,int vCount,
			int publicationRate,int executionTime,String logDir,int processingInterval){
	
		logger= LogManager.getLogger(this.getClass().getSimpleName());
		this.graphId=graphId;
		this.vId=vId;
		this.selectivity=selectivity;
		this.inputRate=inputRate;
		//this.sinkCount=sinkCount;
		//this.sourceCount=sourceCount;
		//this.vCount=vCount;
		this.publicationRate=publicationRate;
		this.executionTime=executionTime;
		this.logDir=logDir;
		this.processingInterval=processingInterval;

		source=false;
		sink=false;
		cleanupCalled=false;
		//initialize curator client for ZK connection
		client=CuratorFrameworkFactory.newClient(ZK_CONNECTOR,
						new ExponentialBackoffRetry(1000, 3));
		client.start();
		
		ipAddr=Util.ipAddressIface("eth0").substring(1);
		logger.info("Vertex:{} will execute on node:{}",vId,ipAddr);
		subscriberSocket=null;
		publisherSockets=new HashMap<String,ZMQ.Socket>();
		initialize(incomingEdges,outgoingEdges);
	}
	
	private void initialize(ArrayList<String> incomingEdges,ArrayList<String> outgoingEdges){
		try{
			//create ZMQ context
			this.context = ZMQ.context(1);
			
			//determine if vertex is source or sink
			if (incomingEdges.size() == 0) {
				source = true;
				logger.info("Vertex:{} is a SOURCE vertex",vId);
			}
			if (outgoingEdges.size() == 0) {
				sink = true;
				pw = new PrintWriter(String.format("%s/%s.csv",
						logDir,vId));
				logger.info("Vertex:{} is a SINK vertex",vId);
			}
			
			//Initialize control SUB socket to receive control signals
			controlSocket=context.socket(ZMQ.SUB);
			controlSocket.setHWM(0);
			String controlConnector = new String(client.getData().forPath(String.format("/%s/control", graphId)));
			controlSocket.connect(controlConnector);
			controlSocket.subscribe(graphId.getBytes());
			logger.info("Vertex:{} will listen to control commands at {}",vId,controlConnector);

			
			//Initialize PUB sockets for outgoing edges
			for (String topic : outgoingEdges) {
				ZMQ.Socket socket = context.socket(ZMQ.PUB);
				int portNum = socket.bindToRandomPort("tcp://*");
				socket.setHWM(0);
				publisherSockets.put(topic, socket);
				logger.info("Vertex:{} will publish topic:{} at {}", vId, topic,
						String.format("tcp://%s:%d", ipAddr, portNum));

				client.create().creatingParentsIfNeeded().forPath(String.format("/%s/topics/%s", graphId, topic),
						String.format("tcp://%s:%d", ipAddr, portNum).getBytes());
			}
			 
			//Initialize SUB sockets for incoming edges
			for (String topic : incomingEdges) {
				if (subscriberSocket == null) {
					subscriberSocket = context.socket(ZMQ.SUB);
					subscriberSocket.setHWM(0);
					while (true) {
						Stat res = client.checkExists().forPath(String.format("/%s/topics/%s", graphId, topic));
						if (res != null) {
							String connector = new String(client.getData().forPath(String.format("/%s/topics/%s", graphId, topic)));
							subscriberSocket.connect(connector);
							subscriberSocket.subscribe(topic.getBytes());
							logger.info("Vertex:{} subscribed to topic:{} at {}", vId, topic, connector);
							break;
						}
						Thread.sleep(100);
					}
				}
			}
			
			//Create /graphId/joined/vId node after initialization is complete
			client.create().creatingParentsIfNeeded().forPath(String.format("/%s/coord/joined/%s", graphId, vId));
			logger.info("Vertex:{} initialized", vId);
		}catch(Exception e){
			logger.error("Vertex:{} caught exception:{}",vId,e.getMessage());
		}
	}
	
	public void run(){
		long startTs=System.currentTimeMillis();
		if(source){
			publish();
		}else{
			process();
		}
		long endTs=System.currentTimeMillis();
		cleanup();
		collectStats(startTs,endTs);
		logger.info("Vertex:{} has exited",vId);
	}
	
	private void publish(){
		try{
			// wait for control signal to begin publishing data
			while (true) {
				String[] parts = controlSocket.recvStr().split(" ");
				if (parts[1].equals(CTRL_CMD_START_PUBLISHING)) {
					logger.info(
							"Vertex:{} received control message:{} on control topic:{}.\nVertex:{} will start publishing data.",
							vId, parts[1], parts[0], vId);
					break;
				}
			}
			// wait for some time for connection set-up
			Thread.sleep(10000);
			
			// publish data
			DataSampleHelper fb = new DataSampleHelper();
			int sleep_interval = 1000 / publicationRate;
			for (int i = 0; i < publicationRate * executionTime; i++) {
				byte[] data = fb.serialize(i + 1, System.currentTimeMillis(), 52);
				for (Entry<String, Socket> entry : publisherSockets.entrySet()) {
					entry.getValue().sendMore(entry.getKey().getBytes());
					entry.getValue().send(data);
				}
				if((i+1)%100==0){
					logger.debug("Vertex:{} sent {} samples", vId, i + 1);
				}
				Thread.sleep(sleep_interval);
			}

			// wait for control signal to exit
			while(true){
				String[] parts = controlSocket.recvStr().split(" ");
				if (parts[1].equals(CTRL_CMD_EXIT)) {
					logger.info("Vertex:{} got control msg:{} on topic:{}.\nVertex:{} will exit.", vId, parts[1], parts[0],vId);
					break;
				}
			}
		}catch(Exception e){
			logger.error("Vertex:{} caught exception:{}",e,e.getMessage());
		}
	}
	
	private void process(){
		poller=context.poller(2);
		poller.register(subscriberSocket, ZMQ.Poller.POLLIN);
		poller.register(controlSocket, ZMQ.Poller.POLLIN);

		logger.info("Vertex:{} will start processing incoming data",vId);
		int count=0;
		while (true) {
			try {
				poller.poll(-1);
				if (poller.pollin(0)) {//process incoming data 
					//receive data
					ZMsg msg= ZMsg.recvMsg(subscriberSocket);
					byte[] data=msg.getLast().getData();
					DataSample sample=DataSampleHelper.deserialize(data);
				
					//increment count
					count++;
				
					//perform bogus operation
					bogus(processingInterval);

					if (sink) {
						// write results to file
						long receiveTs = System.currentTimeMillis();
						long sourceTs = sample.sourceTs();
						pw.println(String.format("%s,%d,%d", vId, sample.sampleId(), receiveTs - sourceTs));
						if (count >= (int) (inputRate * publicationRate * executionTime)) {
							logger.info("Vertex:{} received all messages count:{}.\nVertex:{} will exit", vId, count,vId);
							client.create().forPath(String.format("/%s/coord/sink/%s",graphId,vId));
							break;
						}

					}else{
						// forward data on outgoing edges as per selectivity
						if (selectivity == 1 || (selectivity == .5 && count % 2 == 0)) {
							for (Entry<String, Socket> entry : publisherSockets.entrySet()) {
								entry.getValue().sendMore(entry.getKey().getBytes());
								entry.getValue().send(data);
							}
						}
					}
					
					if(count%100==0){
						logger.debug("Vertex:{} received sample count:{}",vId,count);
                	}
				}
				if (poller.pollin(1)) {//process control command
					String[] parts=controlSocket.recvStr().split(" ");
					if(parts[1].equals(CTRL_CMD_EXIT)){
						logger.info("Vertex:{} got control msg:{} on topic:{}.\nVertex:{} will exit.",
								vId,parts[1],parts[0],vId);
						break;
					}
				}
			}catch (Exception e) {
				logger.error("Vertex:{} caught exception:{}",vId,e.getMessage());
				break;
			}
		}
	}
	
	public void cleanup(){
		if(!cleanupCalled){
			if (pw!=null){
				pw.close();
			}
			controlSocket.setLinger(0);
			controlSocket.close();
			if(poller!=null){
				poller.close();
			}
			if(subscriberSocket!=null){
				subscriberSocket.setLinger(0);
				subscriberSocket.close();
			}
			for(ZMQ.Socket sock: publisherSockets.values()){
				sock.setLinger(0);
				sock.close();
			}
			context.close();
			cleanupCalled=true;
			logger.info("Vertex:{} closed ZMQ sockets and context",vId);
		}
	}

	private void collectStats(long startTs,long endTs){
		SimpleDateFormat formatter= new SimpleDateFormat("HH:mm:ss");
		String startTime= formatter.format(new Date(startTs));
		String endTime= formatter.format(new Date(endTs));

		//collect system utilization metrics 
		logger.info("Vertex:{} will collect system utilization metrics",vId);
		String utilStatsFile=String.format("%s/util_%s.csv",logDir,Util.hostName());
		String command=String.format("sadf -s %s -e %s -U -h -d -- -ur",startTime,endTime);
		Util.executeCommand(command,utilStatsFile);

		//collect network utilization metrics
		logger.info("Vertex:{} will collect network utilization metrics",vId);
		String nwStatsFile=String.format("%s/nw_%s.csv",logDir,Util.hostName());
		command=String.format("sadf -s %s -e %s -U -h -d -- -n DEV",startTime,endTime);
		Util.executeCommand(command,nwStatsFile);
	}

	public static void main(String args[]){
		if(args.length < 6){
			System.out.println("Vertex graphId,vertex_descriptor_string,publicationRate,executionTime,logDir,processingInterval");
			return;
		}
		String graphId=args[0];
		String vertex_descriptor_string=args[1].replace("\\", "");
		String[] parts= vertex_descriptor_string.split(";");
		System.out.println(args[0]);
		System.out.println(vertex_descriptor_string);
		int publicationRate=Integer.parseInt(args[2]);
		int executionTime=Integer.parseInt(args[3]);
		String logDir=args[4];
		int processingInterval=Integer.parseInt(args[5]);
		
		//parse the vertex_descriptor_string
		String vId=parts[0];
		ArrayList<String> subscriptionTopics;
		ArrayList<String> publicationTopics;

		if (parts[1].length()>0){
			String[] incomingEdges=parts[1].split(",");
			subscriptionTopics=new ArrayList<String>(Arrays.asList(incomingEdges));
		}else{
			subscriptionTopics=new ArrayList<String>();
		}

		if (parts[2].length()>0){
			String[] outgoingEdges=parts[2].split(",");
			publicationTopics=new ArrayList<String>(Arrays.asList(outgoingEdges));
		}else{
			publicationTopics=new ArrayList<String>();
		}
		float selectivity=Float.parseFloat(parts[3]);
		float inputRate=Float.parseFloat(parts[4]);
		int sinkCount=Integer.parseInt(parts[5]);
		int sourceCount=Integer.parseInt(parts[6]);
		int vCount=Integer.parseInt(parts[7]);
		
		Vertex v= new Vertex(graphId,vId,
				subscriptionTopics,publicationTopics,
				selectivity,inputRate,
				sinkCount,sourceCount,vCount,
				publicationRate,executionTime,logDir,processingInterval);

		//install hook to handle SIGTERM and SIGINT
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				v.cleanup();
			}
		});

		v.run();
	}

	private void bogus(int processingInterval){
		//fib(22) was benchmarked on BBB and it takes ~1ms on average 
		if (Util.bogusIterations.containsKey(processingInterval)){
			for(int i=0; i< Util.bogusIterations.get(processingInterval);i++){
				Util.fib(22);
			}
		}
	}
}
