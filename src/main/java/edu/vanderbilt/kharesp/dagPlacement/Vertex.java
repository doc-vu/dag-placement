package edu.vanderbilt.kharesp.dagPlacement;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.rti.dds.domain.DomainParticipant;
import com.rti.dds.domain.DomainParticipantFactory;
import com.rti.dds.infrastructure.InstanceHandle_t;
import com.rti.dds.infrastructure.StatusKind;
import com.rti.dds.publication.DataWriter;
import com.rti.dds.publication.Publisher;
import com.rti.dds.subscription.DataReader;
import com.rti.dds.subscription.Subscriber;
import com.rti.dds.topic.Topic;
import com.rti.dds.type.builtin.StringDataReader;
import com.rti.dds.type.builtin.StringDataWriter;
import com.rti.dds.type.builtin.StringTypeSupport;
import com.rti.dds.types.DataSample64B;
import com.rti.dds.types.DataSample64BDataWriter;
import com.rti.dds.types.DataSample64BTypeSupport;


public class Vertex {
	private static final int DOMAIN_ID=1;
	
	private Logger logger;
	private String graphId;
	private String vId;
	private boolean source;
	private boolean sink;
	private float selectivity;
	private float inputRate;
	private int publicationRate;
	private int executionTime;
	private int sinkCount;
	private int processingInterval;
	private int vCount;
	private String logDir;

	private HashMap<String,Topic> subscribingTopics;
	private HashMap<String,DataReader> dataReaders;
	private HashMap<String,Operation> listeners;
	private HashMap<String,Topic> publishingTopics;
	private HashMap<String,DataWriter> dataWriters;

	private DomainParticipant participant;
	private StringDataWriter controlWriter;
	private StringDataReader controlReader;
	private Subscriber subscriber;
	private Publisher publisher;
	private Topic controlTopic;
	private CountDownLatch exitLatch;
	private CountDownLatch sourceLatch;
	private boolean cleanupCalled;
	
	private long startTs;
	private long endTs;
	
	
	public Vertex(String graphId,String vId,
			ArrayList<String> incomingEdges,ArrayList<String> outgoingEdges,
			float selectivity,float inputRate,int sinkCount,int sourceCount,int vCount,
			int publicationRate,int executionTime,String logDir,int processingInterval) throws Exception{
		logger= LogManager.getLogger(this.getClass().getSimpleName());
		this.graphId=graphId;
		this.vId=vId;
		source=false;
		sink=false;
		this.selectivity=selectivity;
		this.inputRate=inputRate;
		this.publicationRate=publicationRate;
		this.executionTime=executionTime;
		this.sinkCount=sinkCount;
		this.vCount=vCount;
		this.cleanupCalled=false;
		this.logDir=logDir;
		this.processingInterval=processingInterval;

		subscribingTopics=new HashMap<String,Topic>();
		dataReaders=new HashMap<String,DataReader>();
		listeners=new HashMap<String,Operation>();

		publishingTopics=new HashMap<String,Topic>();
		dataWriters=new HashMap<String,DataWriter>();
		
		logger.debug("Vertex:{} will be created with:\nin-coming edges:{}\nout-going edges:{}\n",
				vId,Arrays.toString(incomingEdges.toArray()),Arrays.toString(outgoingEdges.toArray()));

		initialize(incomingEdges,outgoingEdges);
	}

	private void initialize(ArrayList<String> incomingEdges,ArrayList<String> outgoingEdges) throws Exception {
		//Create DomainParticipant
		participant = DomainParticipantFactory.TheParticipantFactory.create_participant(DOMAIN_ID,
				DomainParticipantFactory.PARTICIPANT_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
		if (participant == null) {
			logger.error("Vertex:{} failed to create DomainParticipant",vId);
			throw new Exception("create_participant error\n");
		}
		
		logger.debug("Vertex:{} created its DomainParticipant for domain-id:{}",vId,DOMAIN_ID);

		//Register type
		String typeName = DataSample64BTypeSupport.get_type_name();
        DataSample64BTypeSupport.register_type(participant, typeName);
         
		logger.debug("Vertex:{} registered type:{}",vId,typeName);

        //Create Topics
        for (String topicName: incomingEdges) {
        	Topic topic=participant.create_topic(topicName,DataSample64BTypeSupport.get_type_name(),
    				DomainParticipant.TOPIC_QOS_DEFAULT, null,
    				StatusKind.STATUS_MASK_NONE);
    		if (topic == null) {
    			logger.error("Vertex:{} failed to create subscription topic:{}",vId,topicName);
    			throw new Exception("create_topic error\n");
    		}
        	subscribingTopics.put(topicName,topic);
        	logger.debug("Vertex:{} created subscription topic:{}",vId,topicName);

        }
        for(String topicName: outgoingEdges){
        	Topic topic=participant.create_topic(topicName,DataSample64BTypeSupport.get_type_name(),
    				DomainParticipant.TOPIC_QOS_DEFAULT, null,
    				StatusKind.STATUS_MASK_NONE);
    		if (topic == null) {
    			logger.error("Vertex:{} failed to create publication topic:{}",vId,topicName);
    			throw new Exception("create_topic error\n");
    		}
        	publishingTopics.put(topicName, topic);
        	logger.debug("Vertex:{} created publication topic:{}",vId,topicName);
        }

		// Create Control Topic
		controlTopic = participant.create_topic(graphId, StringTypeSupport.get_type_name(),
				DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
		if (controlTopic == null) {
			logger.error("Vertex:{} failed to create control topic:{}", vId, graphId);
			throw new Exception("create_topic error\n");
		}
        logger.debug("Vertex:{} created control topic:{}",vId,graphId);
        
        //Create Subscriber
		subscriber = participant.create_subscriber(DomainParticipant.SUBSCRIBER_QOS_DEFAULT, null,
				StatusKind.STATUS_MASK_NONE);
		if (subscriber == null) {
			logger.error("Vertex:{} failed to create subscriber", vId);
			throw new Exception("create_subscriber error\n");
		}

		logger.debug("Vertex:{} created its subscriber", vId);
       
		//Create DataReaders for incoming topic streams
		for (Entry<String, Topic> pair : subscribingTopics.entrySet()) {
			DataReader reader = subscriber.create_datareader(pair.getValue(), Subscriber.DATAREADER_QOS_DEFAULT, null,
					StatusKind.STATUS_MASK_ALL);
			if (reader == null) {
				logger.error("Vertex:{} failed to create DataReader", vId);
				throw new Exception("create_datareader error\n");
			}
			dataReaders.put(pair.getKey(), reader);
			logger.debug("Vertex:{} created DataReader for subscription topic:{}", vId, pair.getKey());
		}
		//Create Publisher
		publisher = participant.create_publisher(DomainParticipant.PUBLISHER_QOS_DEFAULT, null,
				StatusKind.STATUS_MASK_NONE);
		if (publisher == null) {
			logger.error("Vertex:{} failed to create Publisher", vId);
			throw new Exception("create_publisher error\n");
		}
		logger.debug("Vertex:{} created its publisher", vId);

		//Create DataWriters for outgoing topic streams 
		for (Entry<String, Topic> pair : publishingTopics.entrySet()) {
			DataWriter writer = publisher.create_datawriter(pair.getValue(), Publisher.DATAWRITER_QOS_DEFAULT, null,
					StatusKind.STATUS_MASK_NONE);
			if (writer == null) {
				logger.error("Vertex:{} failed to create DataWriter", vId);
				throw new Exception("create_datawriter error\n");
			}
			dataWriters.put(pair.getKey(), writer);
			logger.debug("Vertex:{} created DataWriter for publication topic:{}", vId, pair.getKey());
		}

        if (incomingEdges.size()==0){
        	source=true;
        	logger.debug("Vertex:{} is a Source vertex", vId);
        }
        if (outgoingEdges.size()==0){
        	sink=true;
        	logger.debug("Vertex:{} is a Sink vertex", vId);
        }

		exitLatch=new CountDownLatch(sinkCount);
		sourceLatch=new CountDownLatch(vCount);
		// create controlTopic DataWriter
		controlWriter = (StringDataWriter) publisher.create_datawriter(controlTopic,
				Publisher.DATAWRITER_QOS_DEFAULT,
				null, StatusKind.STATUS_MASK_NONE);
		if (controlWriter == null) {
			logger.error("Vertex:{} failed to create Control DataWriter", vId);
			throw new Exception("create_datawriter error\n");
		}
		logger.debug("Vertex:{} created its control DataWriter", vId);

		// create controlTopic DataReader
		controlReader = (StringDataReader) subscriber.create_datareader(controlTopic, // Topic
				Subscriber.DATAREADER_QOS_DEFAULT, // QoS
				new ControlListener(graphId, sourceLatch, exitLatch), // Listener
				StatusKind.DATA_AVAILABLE_STATUS); // mask

		if (controlReader == null) {
			logger.error("Vertex:{} failed to create Control DataReader", vId);
			throw new Exception("create_datareader error\n");
		}
		logger.debug("Vertex:{} created its control DataReader", vId);
		
		// send initialization update
		controlWriter.write(String.format("vid_%s",vId),InstanceHandle_t.HANDLE_NIL);
		startTs=System.currentTimeMillis();
	}
	
	public void run(){
		if (source){//If it is a source vertex, start publishing data
			publish();
		}else{//Otherwise register listeners for incoming data
			for (Entry<String,DataReader> pair: dataReaders.entrySet()){
				Operation op=new Operation(graphId,
						pair.getKey(),
						selectivity,
						sink,
						dataWriters,
						logDir,processingInterval);
				pair.getValue().set_listener(op,
						StatusKind.STATUS_MASK_ALL);
				listeners.put(pair.getKey(), op);
				logger.debug("Vertex:{} registered Listener for DataReader:{}",vId,pair.getKey());
			}
		}
		await();
		cleanup();
		collectStats();
	}
	
	private void collectStats(){
		SimpleDateFormat formatter= new SimpleDateFormat("HH:mm:ss");
		String startTime= formatter.format(new Date(startTs));
		String endTime= formatter.format(new Date(endTs));

		//collect system utilization metrics 
		String utilStatsFile=String.format("%s/util_%s.csv",logDir,Util.hostName());
		String command=String.format("sadf -s %s -e %s -U -h -d -- -ur",startTime,endTime);
		Util.executeCommand(command,utilStatsFile);

		//collect network utilization metrics
		String nwStatsFile=String.format("%s/nw_%s.csv",logDir,Util.hostName());
		command=String.format("sadf -s %s -e %s -U -h -d -- -n DEV",startTime,endTime);
		Util.executeCommand(command,nwStatsFile);
	}

	private void await(){
		try {
			if (sink) {
				//poll all DataReaderListeners receive counts
				while(true){
					int receiveCount=0;
					for(Operation op: listeners.values()){
						receiveCount+=op.count.get();
					}
					if (receiveCount>=(int)(inputRate*publicationRate*executionTime)){
						logger.debug("Vertex:{} receied all messages",vId);
						break;
					}
					Thread.sleep(5000);
				}
				logger.info("Vertex:{} sent exit control message",vId);
				controlWriter.write(String.format("sink_%s",vId),InstanceHandle_t.HANDLE_NIL);
				Thread.sleep(5000);
				
			} else {
				exitLatch.await();
			}
			endTs=System.currentTimeMillis();
		} catch (InterruptedException e) {
			logger.error("Vertex:{} caught exception:{}",vId,e.getMessage());
		}
	}

	private void publish(){
		int sleep_interval=1000/publicationRate;
		int count = 0;
		try {
			sourceLatch.await();
			logger.info("Vertex:{} Source vertex will start publishing data...", vId);
			while (count < publicationRate * executionTime) {
				DataSample64B sample = new DataSample64B();
				sample.sample_id = count;
				sample.source_id = 0;
				sample.ts_milisec = System.currentTimeMillis();
				for (DataWriter dw : dataWriters.values()) {
					((DataSample64BDataWriter) dw).write(sample, InstanceHandle_t.HANDLE_NIL);
				}
				if (count % 100 == 0) {
					logger.debug("Vertex:{} published sample:{}", vId, count);
				}
				Thread.sleep(sleep_interval);
				count++;
			}
		} catch (InterruptedException e) {
			logger.error("Vertex:{} caught exception:{}", vId, e.getMessage());
		}
	}

	public void cleanup(){
		if (!cleanupCalled) {
			for (Entry<String, DataReader> pair : dataReaders.entrySet()) {
				subscriber.delete_datareader(pair.getValue());
				listeners.get(pair.getKey()).close_writer();
			}
			if (participant != null) {
				participant.delete_contained_entities();
				DomainParticipantFactory.TheParticipantFactory.delete_participant(participant);
			}
			DomainParticipantFactory.finalize_instance();
			cleanupCalled=true;
		}
	}
	
	
	public static void main(String args[]) throws Exception{
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
