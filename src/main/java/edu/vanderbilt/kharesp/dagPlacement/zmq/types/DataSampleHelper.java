package edu.vanderbilt.kharesp.dagPlacement.zmq.types;

import com.google.flatbuffers.FlatBufferBuilder;

public class DataSampleHelper {
	private FlatBufferBuilder builder;
	public DataSampleHelper(){
		builder= new FlatBufferBuilder(128);
	}

	public byte[] serialize(int sampleId, long sourceTs,int payloadSize){
		builder.clear();
		int payloadOffset= DataSample.createPayloadVector(builder, new byte[payloadSize]);
		int sample=DataSample.createDataSample(builder,sampleId,sourceTs,payloadOffset);
		builder.finish(sample);
		return builder.sizedByteArray();
	}
	
	public static DataSample deserialize(byte[] data){
		java.nio.ByteBuffer buf= java.nio.ByteBuffer.wrap(data);
		return DataSample.getRootAsDataSample(buf);
	}


}
