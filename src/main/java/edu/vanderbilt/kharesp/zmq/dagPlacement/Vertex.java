package edu.vanderbilt.kharesp.zmq.dagPlacement;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.data.Stat;
import org.zeromq.ZMQ;

import edu.vanderbilt.kharesp.dagPlacement.Util;

public class Vertex {
	public static final String ZK_CONNECTOR="129.59.105.159:2181";

	private Logger logger;

	private String graphId;
	private String vId;
	private float selectivity;
	private float inputRate;
	private int sinkCount;
	private int vCount;
	private float publicationRate;
	private int executionTime;
	private String logDir;
	private int processingInterval;

	
	private boolean source;
	private boolean sink;
	private boolean cleanupCalled;
	private CuratorFramework client;
	
	private ZMQ.Context context;
	private String ipAddr;
	private HashMap<String,ZMQ.Socket> publisherSockets; 
	private ZMQ.Socket subscriberSocket;

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
		this.sinkCount=sinkCount;
		this.vCount=vCount;
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
		
		ipAddr=Util.ipAddress();
		subscriberSocket=null;
		publisherSockets=new HashMap<String,ZMQ.Socket>();
		initialize(incomingEdges,outgoingEdges);
	}
	
	private void initialize(ArrayList<String> incomingEdges,ArrayList<String> outgoingEdges){
		this.context = ZMQ.context(1);
		if (incomingEdges.size()==0){
			source=true;
		}
		if(outgoingEdges.size()==0){
			sink=true;
		}

		for(String topic: outgoingEdges){
			ZMQ.Socket socket= context.socket(ZMQ.PUB);
			int portNum=socket.bindToRandomPort("tcp://*");
			socket.setHWM(0);
			publisherSockets.put(topic,socket);
			logger.info("Vertex:{} will publish topic:{} at {}",vId,topic,String.format("tcp://%s:%d",ipAddr,portNum));

			try {
				client.create().creatingParentsIfNeeded().forPath(String.format("/%s/topics/%s",graphId,topic),
						String.format("tcp://%s:%d",ipAddr,portNum).getBytes());
			} catch (Exception e) {
				logger.error("Vertex:{} caught exception:{}",vId,e.getMessage());
			}
		}
		for(String topic: incomingEdges){
			if(subscriberSocket==null){
				subscriberSocket=context.socket(ZMQ.SUB);
				subscriberSocket.setHWM(0);
				while(true){
					try {
						Stat res=client.checkExists().forPath(String.format("/%s/topics/%s",graphId,topic));
						if(res!=null){
							String connector= new String(client.getData().forPath(String.format("/%s/topics/%s",graphId,topic)));
							subscriberSocket.connect(connector);
							subscriberSocket.subscribe(topic.getBytes());
							logger.info("Vertex:{} subscribed to topic:{} at {}",vId,topic,connector);
							break;
						}
						Thread.sleep(100);
					} catch (Exception e) {
						logger.error("Vertex:{} caught exception:{}",vId,e.getMessage());
					}
				}
			}
		}

		try {
			client.create().creatingParentsIfNeeded().forPath(String.format("/%s/coord/joined/%s",graphId,vId));
			logger.info("Vertex:{} initialized",vId);
		} catch (Exception e) {
			logger.error("Vertex:{} caught exception:{}",vId,e.getMessage());
		}
	}
	
	public void run(){
		if(source){
			publish();
		}else{
			process();
		}
	}
	
	private void publish(){
		
	}
	
	private void process(){
		
	}
	
	public void cleanup(){
		if(!cleanupCalled){
			if(subscriberSocket!=null){
				subscriberSocket.setLinger(0);
				subscriberSocket.close();
			}
			for(ZMQ.Socket sock: publisherSockets.values()){
				sock.setLinger(0);
				sock.close();
			}
			context.close();
		}
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

}
