package ch.cern.alice.o2.kafka.utils;

import java.util.List;
import java.util.Map;

public class YamlAggregatorConfig{
		private Map<String,String> general;
		private Map<String,String> kafka_config;
		private Map<String,String> aggregation_config;
		private List<Map<String,String>> avg_filter;
		private List<Map<String,String>> sum_filter;
		private List<Map<String,String>> min_filter;
		private List<Map<String,String>> max_filter;
		private Map<String,String> stats_config;
		
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
		
		public List<Map<String,String>> getAvg_filter(){
			return avg_filter;
		}
		
		public void setAvg_filter(List<Map<String,String>> new_avg_filter) {
			avg_filter = new_avg_filter;
		}

		public List<Map<String,String>> getSum_filter(){
			return sum_filter;
		}
		
		public void setSum_filter(List<Map<String,String>> new_sum_filter) {
			sum_filter = new_sum_filter;
		}

		public List<Map<String,String>> getMin_filter(){
			return min_filter;
		}
		
		public void setMin_filter(List<Map<String,String>> new_min_filter) {
			min_filter = new_min_filter;
		}

		public List<Map<String,String>> getMax_filter(){
			return max_filter;
		}
		
		public void setMax_filter(List<Map<String,String>> new_max_filter) {
			max_filter = new_max_filter;
		}

		public Map<String,String> getStats_config() {
			return stats_config;
		}
		
		public void setStats_config(Map<String,String> new_gen) {
			stats_config = new_gen;
		}
}