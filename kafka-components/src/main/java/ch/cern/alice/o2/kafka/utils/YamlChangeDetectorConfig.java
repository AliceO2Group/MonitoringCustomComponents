package ch.cern.alice.o2.kafka.utils;

import java.util.Map;

public class YamlChangeDetectorConfig{
		private Map<String,String> general;
		private Map<String,String> kafka_config;
		private Map<String,String> detector;
		private Map<String,String> stats_config;
		
		public YamlChangeDetectorConfig() { }
		
		public Map<String,String> getGeneral() {
			return general;
		}
		
		public void setGeneral(Map<String,String> new_gen) {
			general = new_gen;
		}
		
		public Map<String,String> getkafka_config(){
			return kafka_config;
		}
		
		public void setkafka_config(Map<String,String> k_config) {
			kafka_config = k_config;
		}
		
		public Map<String,String> getDetector(){
			return detector;
		}
		
		public void setDetector(Map<String,String> new_det) {
			detector = new_det;
		}

		public Map<String,String> getStats_config() {
			return stats_config;
		}
		
		public void setStats_config(Map<String,String> new_gen) {
			stats_config = new_gen;
		}

}