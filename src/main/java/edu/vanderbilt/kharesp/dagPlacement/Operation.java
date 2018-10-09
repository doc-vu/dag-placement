package edu.vanderbilt.kharesp.dagPlacement;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
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
	private long count;
	private boolean sink;
	private String listenerId;
	private HashMap<String,DataWriter> dataWriters;
	private DataSample64BSeq dataSeq;
	private SampleInfoSeq infoSeq;
	private PrintWriter pw;
	
	public Operation(String graphId,String listenerId,boolean sink,HashMap<String,DataWriter> dataWriters){
		logger= LogManager.getLogger(this.getClass().getSimpleName());
		this.listenerId=listenerId;
		this.sink=sink;
		this.dataWriters=dataWriters;

		count=0;
		dataSeq=new DataSample64BSeq();
		infoSeq=new SampleInfoSeq();
		
		//Ensure directory for logging this graph's output exists
		String current_directory=Paths.get(".").toAbsolutePath().normalize().toString();
		Path path = Paths.get(String.format("%s/log/dag/%s",current_directory,graphId));
        if (!Files.exists(path)) {
            try {
                Files.createDirectories(path);
            } catch (IOException e) {
            	logger.error("DataReaderListener:{} caught exception:{}",listenerId,e.getMessage());
            }
        }
		try {
			if (sink) {
				pw = new PrintWriter(String.format("%s/log/dag/%s/%s.csv",
						current_directory, graphId, listenerId));
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
                	DataSample64B sample=dataSeq.get(i);
                	if(count%100==0){
                		logger.debug("DataReaderListener:{} sample count:{}",listenerId,count);
                	}
                	if (sink){
                		long receive_ts=System.currentTimeMillis();
                		long source_ts=sample.ts_milisec;
                		pw.println(String.format("%s,%d,%d",listenerId,sample.sample_id,receive_ts-source_ts));
                	}else{
                		for (DataWriter dw: dataWriters.values()){
                			((DataSample64BDataWriter)dw).write(sample,InstanceHandle_t.HANDLE_NIL);
                		}
                	}
                	count++;
                }
            }
        } catch (RETCODE_NO_DATA noData) {

        } finally {
            dr.return_loan(dataSeq, infoSeq);
        }
	}

	public void close_writer(){
		if (sink){
			pw.close();
		}
	}
}
