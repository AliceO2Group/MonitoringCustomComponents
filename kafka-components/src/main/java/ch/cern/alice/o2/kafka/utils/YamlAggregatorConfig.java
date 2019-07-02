package ch.cern.alice.o2.kafka.utils;

import java.util.Map;

public class YamlAggregatorConfig{
		private Map<String,String> general;
		private Map<String,String> topics;
		private Map<String,String> kafka_config;
		private Map<String,Map<String,String>[]> aggregators;
		
		public YamlAggregatorConfig() { }
		
		public Map<String,String> getGeneral() {
			return general;
		}
		
		public void setGeneral(Map<String,String> new_gen) {
			general = new_gen;
		}
		
		public Map<String,String> getTopics(){
			return topics;
		}
		
		public void setTopics(Map<String,String> k_topics) {
			topics = k_topics;
		}
		
		public Map<String,String> getkafka_config(){
			return kafka_config;
		}
		
		public void setkafka_config(Map<String,String> k_config) {
			kafka_config = k_config;
		}
		
		public Map<String,Map<String,String>[]> getAggregators(){
			return aggregators;
		}
		
		public void setAggregators(Map<String,Map<String,String>[]> aggr) {
			aggregators = aggr;
		}
}