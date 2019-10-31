package ch.cern.alice.o2.kafka.utils;

public class SimplePair{
		public String key;
		public String value;
		
		public SimplePair() { }
		
		public SimplePair(String z, String o) {
			key = z;
			value = o;
		}
		
		public String toString() {
			return key+"-> "+value;
		}
}