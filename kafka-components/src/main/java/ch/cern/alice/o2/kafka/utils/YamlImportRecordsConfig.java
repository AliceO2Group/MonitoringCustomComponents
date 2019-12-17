package ch.cern.alice.o2.kafka.utils;

import java.util.List;
import java.util.Map;

public class YamlImportRecordsConfig{
		private Map<String,String> general;
		private Map<String,String> kafka_config;
		private Map<String,String> component_config;
		private List<Map<String,String>> database_filter;
		private List<Map<String,String>> onoff_filter;
		private List<Map<String,String>> aggregator_filter;
		private Map<String,String> stats_config;
		
		public YamlImportRecordsConfig() { }
		
		public Map<String,String> getGeneral() {
			return general;
		}
		
		public void setGeneral(Map<String,String> new_gen) {
			general = new_gen;
		}
		
		public Map<String,String> getKafka_config(){
			return kafka_config;
		}
		
		public void setKafka_config(Map<String,String> k_config) {
			kafka_config = k_config;
		}
		
		public Map<String,String> getComponent_config(){
			return component_config;
		}
		
		public void setComponent_config(Map<String,String> new_compconf) {
			component_config = new_compconf;
		}

		public List<Map<String,String>> getDatabase_filter(){
			return database_filter;
		}
		
		public void setDatabase_filter(List<Map<String,String>> new_db_filter) {
			database_filter = new_db_filter;
		}

		public List<Map<String,String>> getOnOff_filter(){
			return onoff_filter;
		}
		
		public void setOnOff_filter(List<Map<String,String>> new_onoff_filter) {
			onoff_filter = new_onoff_filter;
		}

		public List<Map<String,String>> getAggregator_filter(){
			return aggregator_filter;
		}
		
		public void setaggregator_filter(List<Map<String,String>> new_aggregator_filter) {
			aggregator_filter = new_aggregator_filter;
		}

		public Map<String,String> getStats_config() {
			return stats_config;
		}
		
		public void setStats_config(Map<String,String> new_gen) {
			stats_config = new_gen;
		}
}