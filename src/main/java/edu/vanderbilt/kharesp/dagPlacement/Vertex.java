package edu.vanderbilt.kharesp.dagPlacement;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map.Entry;
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
import com.rti.dds.types.DataSample64B;
import com.rti.dds.types.DataSample64BDataWriter;
import com.rti.dds.types.DataSample64BTypeSupport;


public class Vertex {
	private static final int DOMAIN_ID=0;
	
	private Logger logger;
	private String graphId;
	private String vId;
	private boolean source;
	private boolean sink;
	private HashMap<String,Topic> subscribingTopics;
	private HashMap<String,DataReader> dataReaders;
	private HashMap<String,Operation> listeners;
	private HashMap<String,Topic> publishingTopics;
	private HashMap<String,DataWriter> dataWriters;

	private DomainParticipant participant;
	private Subscriber subscriber;
	private Publisher publisher;
	
	
	public Vertex(String graphId,String vId,ArrayList<String> incomingEdges,ArrayList<String> outgoingEdges) throws Exception{
		logger= LogManager.getLogger(this.getClass().getSimpleName());
		this.graphId=graphId;
		this.vId=vId;
		source=false;
		sink=false;

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
        
        if (incomingEdges.size()==0){
        	source=true;
        	//No need to create a Subscriber
        }else{
        	//Create Subscriber 
        	subscriber = participant.create_subscriber(DomainParticipant.SUBSCRIBER_QOS_DEFAULT,
    				null,
    				StatusKind.STATUS_MASK_NONE);
    		if (subscriber == null) {
    			logger.error("Vertex:{} failed to create subscriber",vId);
    			throw new Exception("create_subscriber error\n");
    		}

        	logger.debug("Vertex:{} created its containing subscriber",vId);

    		for (Entry<String,Topic> pair: subscribingTopics.entrySet()){
    			DataReader reader= subscriber.create_datareader(pair.getValue(),Subscriber.DATAREADER_QOS_DEFAULT,
    					null,StatusKind.STATUS_MASK_ALL);
    			if(reader==null){
    				logger.error("Vertex:{} failed to create DataReader",vId);
    				throw new Exception("create_datareader error\n");
    			}
    			dataReaders.put(pair.getKey(), reader);
    			logger.debug("Vertex:{} created DataReader for subscription topic:{}",vId,pair.getKey());
    		}
        }
        if (outgoingEdges.size()==0){
        	sink=true;
        	//No need to create a Publisher
        }else{
        	//Create Publisher
        	publisher= participant.create_publisher(DomainParticipant.PUBLISHER_QOS_DEFAULT,
    				null,
    				StatusKind.STATUS_MASK_NONE);
    		if (publisher == null) {
    			logger.error("Vertex:{} failed to create Publisher",vId);
    			throw new Exception("create_publisher error\n");
    		}	
        	logger.debug("Vertex:{} created its containing publisher",vId);

    		for (Entry<String, Topic> pair: publishingTopics.entrySet()){
    			DataWriter writer= publisher.create_datawriter(pair.getValue(),Publisher.DATAWRITER_QOS_DEFAULT,
    				null,StatusKind.STATUS_MASK_NONE);
    			if(writer==null){
    				logger.error("Vertex:{} failed to create DataWriter",vId);
    				throw new Exception("create_datawriter error\n");
    			}
    			dataWriters.put(pair.getKey(),writer);
    			logger.debug("Vertex:{} created DataWriter for publication topic:{}",vId,pair.getKey());
    		}
        }
	}
	
	public void run(){
		if (source){//If it is a source vertex, start publishing data
			publish();
		}else{//Otherwise register listeners for incoming data
			for (Entry<String,DataReader> pair: dataReaders.entrySet()){
				Operation op=new Operation(graphId,pair.getKey(),sink,dataWriters);
				pair.getValue().set_listener(op,
						StatusKind.STATUS_MASK_ALL);
				listeners.put(pair.getKey(), op);
				logger.debug("Vertex:{} registered Listener for DataReader:{}",vId,pair.getKey());
			}
			while(true){
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					logger.error("Vertex:{} caught exception:{}",vId,e.getMessage());
					break;
				}
			}
		}
		cleanup();
	}

	private void publish(){
		logger.info("Vertex:{} is a source vertex. Will start publishing data...", vId);
		int count = 0;
		while(count<1200) {
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
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				logger.error("Vertex:{} caught exception:{}", vId, e.getMessage());
			}
			count++;
		}
	}

	public void cleanup(){
		for (Entry<String,DataReader> pair: dataReaders.entrySet()){
			subscriber.delete_datareader(pair.getValue());
			listeners.get(pair.getKey()).close_writer();
		}
		if (participant != null) {
			participant.delete_contained_entities();
			DomainParticipantFactory.TheParticipantFactory.delete_participant(participant);
		}
		DomainParticipantFactory.finalize_instance();
	}
	
	public static void main(String args[]) throws Exception{
		if(args.length < 2){
			System.out.println("Vertex graphId,vertex_descriptor_string");
			return;
		}
		String graphId=args[0];
		String[] parts= args[1].split(";");
		String vId=parts[0];
		ArrayList<String> subscriptionTopics;
		ArrayList<String> publicationTopics;

		if (parts[1].length()>0){
			String[] incomingEdges=parts[1].split(",");
			subscriptionTopics=new ArrayList<String>(Arrays.asList(incomingEdges));
		}else{
			subscriptionTopics=new ArrayList<String>();
		}

		if (parts.length==3){
			String[] outgoingEdges=parts[2].split(",");
			publicationTopics=new ArrayList<String>(Arrays.asList(outgoingEdges));
		}else{
			publicationTopics=new ArrayList<String>();
		}
		
		Vertex v= new Vertex(graphId,vId,subscriptionTopics,publicationTopics);
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
