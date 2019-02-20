package edu.vanderbilt.kharesp.dagPlacement.dds;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.retry.ExponentialBackoffRetry;
import com.rti.dds.domain.DomainParticipant;
import com.rti.dds.domain.DomainParticipantFactory;
import com.rti.dds.infrastructure.InstanceHandle_t;
import com.rti.dds.infrastructure.StatusKind;
import com.rti.dds.publication.Publisher;
import com.rti.dds.topic.Topic;
import com.rti.dds.type.builtin.StringDataWriter;
import com.rti.dds.type.builtin.StringTypeSupport;
import edu.vanderbilt.kharesp.dagPlacement.util.Util;

public class Controller {
	public static void main(String args[]){
		try {
			if(args.length<2){
				System.out.println("Usage: Controller zkConnector domainId");
				return;
			}
			String zkConnector=args[0];
			int domainId=Integer.parseInt(args[1]);

			// start connection to ZK
			CuratorFramework client = CuratorFrameworkFactory.newClient(zkConnector,
					new ExponentialBackoffRetry(1000, 3));
			client.start();
			DistributedBarrier startBarrier = new DistributedBarrier(client, String.format("/dom%d/barriers/start",domainId));
			DistributedBarrier endBarrier = new DistributedBarrier(client, String.format("/dom%d/barriers/end",domainId));
			DistributedBarrier exitBarrier = new DistributedBarrier(client, String.format("/dom%d/barriers/exit",domainId));

			// create control command publisher
			// create domain participant
			DomainParticipant participant = DomainParticipantFactory.TheParticipantFactory.create_participant(
					domainId, DomainParticipantFactory.PARTICIPANT_QOS_DEFAULT, null,
					StatusKind.STATUS_MASK_NONE);
			// create control topic
			Topic controlTopic = participant.create_topic(Util.CONTROL_TOPIC, StringTypeSupport.get_type_name(),
					DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
			// create publisher
			Publisher publisher = participant.create_publisher(DomainParticipant.PUBLISHER_QOS_DEFAULT, null,
					StatusKind.STATUS_MASK_NONE);
			// create datawriter
			StringDataWriter controlWriter = (StringDataWriter) publisher.create_datawriter(controlTopic,
					Publisher.DATAWRITER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

			System.out.println("Controller will wait for all DAGs to initialize");
			//wait for all DAGs to initialize
			startBarrier.waitOnBarrier();
			System.out.println("All DAGs have initialized. Opening start barrier");
			controlWriter.write(Util.CTRL_CMD_START_PUBLISHING, InstanceHandle_t.HANDLE_NIL);
			//wait for all DAGs to finish execution
			System.out.println("Controller will wait for all DAGs to finish execution");
			endBarrier.waitOnBarrier();
			System.out.println("All DAGs have finished execution.Opening end barrier");
			controlWriter.write(Util.CTRL_CMD_EXIT, InstanceHandle_t.HANDLE_NIL);

			// cleanup
			client.close();
			participant.delete_contained_entities();
			DomainParticipantFactory.TheParticipantFactory.delete_participant(participant);
			DomainParticipantFactory.finalize_instance();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
