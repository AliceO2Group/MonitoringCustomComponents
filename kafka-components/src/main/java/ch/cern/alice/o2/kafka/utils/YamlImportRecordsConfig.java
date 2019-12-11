package ch.cern.alice.o2.kafka.utils;

import java.util.Map;

public class YamlImportRecordsConfig{
		private Map<String,String> general;
		private Map<String,String> kafka_config;
		private Map<String,String> import_config;
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
		
		public Map<String,String> getImport_config(){
			return import_config;
		}
		
		public void setImport_config(Map<String,String> new_importconf) {
			import_config = new_importconf;
		}

		public Map<String,String> getStats_config() {
			return stats_config;
		}
		
		public void setStats_config(Map<String,String> new_gen) {
			stats_config = new_gen;
		}
}