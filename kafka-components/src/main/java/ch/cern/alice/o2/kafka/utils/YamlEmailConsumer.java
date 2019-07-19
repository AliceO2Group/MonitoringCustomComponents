package ch.cern.alice.o2.kafka.utils;

import java.util.Map;

public class YamlEmailConsumer{
		private Map<String,String> general;
		private Map<String,String> kafka_consumer_config;
		private Map<String,String> email_config;
		private Map<String,String> stats_config;
		
		public YamlEmailConsumer() { }
		
		public Map<String,String> getGeneral() {
			return general;
		}
		
		public void setGeneral(Map<String,String> new_gen) {
			general = new_gen;
		}
		
		public Map<String,String> getKafka_consumer_config(){
			return kafka_consumer_config;
		}
		
		public void setKafka_consumer_config(Map<String,String> k_config) {
			kafka_consumer_config = k_config;
		}
		
		public Map<String,String> getEmail_config() {
			return email_config;
		}
		
		public void setEmail_config(Map<String,String> new_gen) {
			email_config = new_gen;
		}
		
		public Map<String,String> getStats_config() {
			return stats_config;
		}
		
		public void setStats_config(Map<String,String> new_gen) {
			stats_config = new_gen;
		}
}