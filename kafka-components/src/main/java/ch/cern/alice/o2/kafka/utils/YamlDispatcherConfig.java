package ch.cern.alice.o2.kafka.utils;

import java.util.Map;

public class YamlDispatcherConfig{
		private Map<String,String> general;
		private String output_avg_topic;
		private String output_sum_topic;
		private String output_min_topic;
		private String output_max_topic;
		private String output_other_topic;
		private String input_topic;
		private Map<String,String> kafka_config;
		private Map<String,Map<String,String>[]> selection;
		
		public YamlDispatcherConfig() { }
		
		public Map<String,String> getGeneral() {
			return general;
		}
		
		public void setGeneral(Map<String,String> new_gen) {
			general = new_gen;
		}
		
		public String getOutput_avg_topic() {
			return output_avg_topic;
		}
		
		public void setOutput_avg_topic(String o_topic) {
			output_avg_topic = o_topic;
		}
		
		public String getOutput_sum_topic() {
			return output_sum_topic;
		}
		
		public void setOutput_sum_topic(String o_topic) {
			output_sum_topic = o_topic;
		}
		
		public String getOutput_max_topic() {
			return output_max_topic;
		}
		
		public void setOutput_max_topic(String o_topic) {
			output_max_topic = o_topic;
		}
		
		public String getOutput_min_topic() {
			return output_min_topic;
		}
		
		public void setOutput_min_topic(String o_topic) {
			output_min_topic = o_topic;
		}
		
		public String getOutput_other_topic() {
			return output_other_topic;
		}
		
		public void setOutput_other_topic(String o_topic) {
			output_other_topic = o_topic;
		}
		
		public String getInput_topic() {
			return input_topic;
		}
		
		public void setInput_topic(String i_topic) {
			input_topic = i_topic;
		}
		
		public Map<String,String> getkafka_config(){
			return kafka_config;
		}
		
		public void setkafka_config(Map<String,String> k_config) {
			kafka_config = k_config;
		}
		
		public Map<String,Map<String,String>[]> getSelection(){
			return selection;
		}
		
		public void setSelection(Map<String,Map<String,String>[]> sel) {
			selection = sel;
		}
}