package ch.cern.alice.o2.kafka.utils;

import java.util.Map;

public class YamlAggregatorConfig{
		private Map<String,String> general;
		private Map<String,String> kafka_config;
		private Map<String,String> aggregation_config;
		
		public YamlAggregatorConfig() { }
		
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
		
		public Map<String,String> getAggregation_config(){
			return aggregation_config;
		}
		
		public void setAggregators(Map<String,String> aggr) {
			aggregation_config = aggr;
		}
}