package ch.cern.alice.o2.kafka.utils;

import java.nio.ByteBuffer;

public class AvgPair{
		public Double value;
		public Integer count;
		
		public AvgPair(Double v, Integer c) {
			value = v;
			count = c;
		}
		
		public AvgPair() {
			value = (double) 0;
			count = (int) 0;
		}
		
		public Double getAverage() {
			Double avg = this.value / 	this.count;
			//System.out.println("("+value.toString()+"/"+count.toString()+"="+avg.toString());
			return avg;		
		}
		
		public String toString() {
			return "("+value.toString()+","+count.toString()+")";
		}
		
		public AvgPair add( AvgPair pair) {
			value += pair.value;
			count += pair.count;
			return this;
		}
		
		public byte [] serialize() {
			ByteBuffer buffer = ByteBuffer.allocate(12);
	        buffer.putDouble(value);
	        buffer.putInt(count);
	        return buffer.array();
		}
		
		public AvgPair deserialize(byte [] bytes) {
			ByteBuffer buffer = ByteBuffer.wrap(bytes);
	    	value = buffer.getDouble();
	    	count = buffer.getInt();
	    	return this;
		}
}