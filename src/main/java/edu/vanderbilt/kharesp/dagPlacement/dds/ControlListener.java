package edu.vanderbilt.kharesp.dagPlacement.dds;

import java.util.concurrent.CountDownLatch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.rti.dds.infrastructure.RETCODE_NO_DATA;
import com.rti.dds.infrastructure.ResourceLimitsQosPolicy;
import com.rti.dds.infrastructure.StringSeq;
import com.rti.dds.subscription.DataReader;
import com.rti.dds.subscription.DataReaderAdapter;
import com.rti.dds.subscription.InstanceStateKind;
import com.rti.dds.subscription.SampleInfo;
import com.rti.dds.subscription.SampleInfoSeq;
import com.rti.dds.subscription.SampleStateKind;
import com.rti.dds.subscription.ViewStateKind;
import com.rti.dds.type.builtin.StringDataReader;

import edu.vanderbilt.kharesp.dagPlacement.util.Util;

public class ControlListener extends DataReaderAdapter {
	private Logger logger;
	private String listenerId;
	private CountDownLatch exitLatch;
	private CountDownLatch sourceLatch;
	private SampleInfoSeq infoSeq;
	private StringSeq dataSeq;

	public ControlListener(String listenerId,
			CountDownLatch sourceLatch,CountDownLatch exitLatch){
		logger= LogManager.getLogger(this.getClass().getSimpleName());
		this.listenerId=listenerId;
		this.exitLatch=exitLatch;
		this.sourceLatch=sourceLatch;
		this.infoSeq=new SampleInfoSeq();
		this.dataSeq=new StringSeq();
	    logger.debug("ControlListener:{} initialized",listenerId);
	}

	public void on_data_available(DataReader reader) {
		StringDataReader dr= (StringDataReader)reader;
		try{
			dr.take(dataSeq, infoSeq,
	                ResourceLimitsQosPolicy.LENGTH_UNLIMITED,
	                SampleStateKind.ANY_SAMPLE_STATE,
	                ViewStateKind.ANY_VIEW_STATE,
	                InstanceStateKind.ANY_INSTANCE_STATE);

			 for(int i = 0; i < dataSeq.size(); ++i) {
	                SampleInfo info = (SampleInfo)infoSeq.get(i);
	                if (info.valid_data) {
	                	String update=(String) dataSeq.get(i);
	                	if (update.equals(Util.CTRL_CMD_EXIT)){
	                		logger.info("ControlListener:{} got command:{}",listenerId,
	                				update);
	                		exitLatch.countDown();
	                	}
	                	if(update.equals(Util.CTRL_CMD_START_PUBLISHING)){
	                		logger.info("ControlListener:{} got command:{}",listenerId,update);
	                		sourceLatch.countDown();
	                		logger.info("ControlListener:{} opened sourceLatch",listenerId);
	                	}
	                }
			 }
		}catch (RETCODE_NO_DATA noData) {

        } finally {
            dr.return_loan(dataSeq, infoSeq);
        }
	}
}

		
