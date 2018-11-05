package edu.vanderbilt.kharesp.dagPlacement;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.rti.dds.infrastructure.InstanceHandle_t;
import com.rti.dds.infrastructure.RETCODE_NO_DATA;
import com.rti.dds.infrastructure.ResourceLimitsQosPolicy;
import com.rti.dds.publication.DataWriter;
import com.rti.dds.subscription.DataReader;
import com.rti.dds.subscription.DataReaderAdapter;
import com.rti.dds.subscription.InstanceStateKind;
import com.rti.dds.subscription.SampleInfo;
import com.rti.dds.subscription.SampleInfoSeq;
import com.rti.dds.subscription.SampleStateKind;
import com.rti.dds.subscription.ViewStateKind;
import com.rti.dds.types.DataSample64B;
import com.rti.dds.types.DataSample64BDataReader;
import com.rti.dds.types.DataSample64BDataWriter;
import com.rti.dds.types.DataSample64BSeq;

public class Operation extends DataReaderAdapter{
	private Logger logger;
	private HashMap<String,DataWriter> dataWriters;
	private DataSample64BSeq dataSeq;
	private SampleInfoSeq infoSeq;

	private String listenerId;
	private boolean sink;
	private float selectivity;
	private int executionInterval;

	public AtomicInteger count;
	private PrintWriter pw;
	
	public Operation(String graphId,
			String listenerId,
			float selectivity,
			boolean sink,
			HashMap<String,DataWriter> dataWriters,
			String logDir,int executionInterval){
		logger= LogManager.getLogger(this.getClass().getSimpleName());

		this.listenerId=listenerId;
		this.selectivity=selectivity;
		this.executionInterval=executionInterval;
		this.sink=sink;

		this.dataWriters=dataWriters;

		count=new AtomicInteger(0);
		dataSeq=new DataSample64BSeq();
		infoSeq=new SampleInfoSeq();
		
		//Ensure directory for logging this graph's output exists
		Path path = Paths.get(logDir);
        if (!Files.exists(path)) {
            try {
                Files.createDirectories(path);
            } catch (IOException e) {
            	logger.error("DataReaderListener:{} caught exception:{}",listenerId,e.getMessage());
            }
        }
		try {
			if (sink) {
				pw = new PrintWriter(String.format("%s/%s.csv",
						logDir, listenerId));
			}
		} catch (FileNotFoundException e) {
			logger.error("DataReaderListener:{} caught exception:{}",listenerId,e.getMessage());
		}
	}
	
	public void on_data_available(DataReader reader){
		DataSample64BDataReader dr= (DataSample64BDataReader) reader;
		try {
            dr.take(
                dataSeq, infoSeq,
                ResourceLimitsQosPolicy.LENGTH_UNLIMITED,
                SampleStateKind.ANY_SAMPLE_STATE,
                ViewStateKind.ANY_VIEW_STATE,
                InstanceStateKind.ANY_INSTANCE_STATE);

            for(int i = 0; i < dataSeq.size(); ++i) {
                SampleInfo info = (SampleInfo)infoSeq.get(i);
                
                if (info.valid_data) {
                	count.incrementAndGet();
                	DataSample64B sample=dataSeq.get(i);
                	//Perform bogus operation
                	//if (!sink){
                	long currTime=System.currentTimeMillis();
                	if (executionInterval>0){
                		while(true){
                			long newTime=System.currentTimeMillis();
                			if ((newTime-currTime)>executionInterval){
                				break;
                			}
                		}

                		//Process p = Runtime.getRuntime().exec(String.format("stress-ng --cpu 1 --cpu-method gray --cpu-ops %d",executionInterval));
                		//p.waitFor();
                	}
                	//}
                	if(count.get()%100==0){
                		logger.debug("DataReaderListener:{} sample count:{}",listenerId,count);
                	}
                	if(selectivity==1 || (selectivity==.5 && count.get()%2==0)){
						if (sink) {
							long receive_ts = System.currentTimeMillis();
							long source_ts = sample.ts_milisec;
							pw.println(String.format("%s,%d,%d", listenerId, sample.sample_id, receive_ts - source_ts));
						} else {
							for (DataWriter dw : dataWriters.values()) {
								((DataSample64BDataWriter) dw).write(sample, InstanceHandle_t.HANDLE_NIL);
							}
						}
                	}
                }
            }
        } catch (RETCODE_NO_DATA noData) {

        } /*
		catch (IOException e) {
        	logger.error("DataReaderListener:{} caught exception:{}",listenerId,e.getMessage());
		} catch (InterruptedException e) {
        	logger.error("DataReaderListener:{} caught exception:{}",listenerId,e.getMessage());
		} */finally {
            dr.return_loan(dataSeq, infoSeq);
        }
	}

	public void close_writer(){
		if (sink){
			pw.close();
		}
	}
}
