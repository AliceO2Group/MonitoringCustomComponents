package ch.cern.alice.o2.kafka.utils;

import java.util.Map;

public class YamlDispatcherConfig{
		private Map<String,String> general;
		private Map<String,String> kafka_config;
		private Map<String,String> topics;
		private Map<String,Map<String,String>[]> selection;
		
		public YamlDispatcherConfig() { }
		
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
		
		public Map<String,String> getTopics(){
			return topics;
		}
		
		public void setTopics(Map<String,String> new_topics) {
			topics = new_topics;
		}

		public Map<String,Map<String,String>[]> getSelection(){
			return selection;
		}
		
		public void setSelection(Map<String,Map<String,String>[]> sel) {
			selection = sel;
		}
}