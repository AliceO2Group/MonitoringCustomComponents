/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ch.cern.alice.o2.kafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;

import static net.sourceforge.argparse4j.impl.Arguments.store;

import java.io.File;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import ch.cern.alice.o2.kafka.utils.KafkaLineProtocol;
import ch.cern.alice.o2.kafka.utils.YamlRouterConfig;

public final class Router {
	private static Set<String> sDatabaseFilterConfig = null;
	private static Set<String> sOnOffFilterConfig    = null;
	private static Set<String> sAggregatorFilterConfig = null;

	private static Map<String,Set<String>> msDatabaseFilterConfig = null;
	private static Map<String,Set<String>> msOnOffFilterConfig    = null;
	private static Map<String,Set<String>> msAggregatorFilterConfig = null;

	private static String statsEndpointHostname = "";
	private static int statsEndpointPort = 0;
	private static String statsType;
	private static long statsPeriodMs = 0;
	private static InetAddress statsAddress = null;
	private static boolean statsEnabled = false;
	private static DatagramSocket datagramSocket;
	private static long receivedRecords = 0;
	private static long sentDbRecords = 0;
	private static long sentOnOffRecords = 0;
	private static long sentAggrRecords = 0;
	private static long startMs = 0;

	private static String KEY_SEPARATOR = "#";
	private static String FIELD_SEPARATOR = ",";
	private static String TAG_SEPARATOR = ",";

	private static Logger logger = LoggerFactory.getLogger(Router.class); 
    private static String ARGPARSE_CONFIG = "config";
    private static String GENERAL_LOGFILENAME_CONFIG = "log4jfilename";
    private static String TOPICS_INPUT_CONFIG = "topic.input";
	private static String TOPICS_OUTPUT_DATABASE_CONFIG = "topic.output.database";
	private static String TOPICS_OUTPUT_ONOFF_CONFIG = "topic.output.onoff";
	private static String TOPICS_OUTPUT_AGGREGATOR_CONFIG = "topic.output.aggregator";
	private static String ARGPARSE_SELECTION_MEASUREMENT_KEY = "measurement";
	private static String ARGPARSE_SELECTION_FIELDNAME_KEY = "field.name";
	
	/* Stats parameters */
	private static final String STATS_TYPE_INFLUXDB = "influxdb";
	private static final String DEFAULT_STATS_TYPE = STATS_TYPE_INFLUXDB;
	private static final String DEFAULT_STATS_PERIOD = "10000";
	private static final String DEFAULT_STATS_ENABLED = "false";
	private static final String DEFAULT_STATS_HOSTNAME = "localhost";
	private static final String DEFAULT_STATS_PORT = "8090";

	private static String DEFAULT_NUM_STREAM_THREADS_CONFIG = "1";
	private static String DEFAULT_APPLICATION_ID_CONFIG = "streams-app-import-records";
	private static String DEFAULT_CLIENT_ID_CONFIG = "streams-client-import-records";
	private static String DEFAULT_CLIENT_DESCRIPTION = "This tool is used to import records and convert them in the kafka protocol.";

	private static String THREAD_NAME = "import-records-shutdown-hook";

	public static List<String> getFilteredDatabaseRecords(String lp){
		List<String> records = new ArrayList<String>();
		String meas = lp.split(",")[0];
		if(msDatabaseFilterConfig.keySet().contains(meas)){
			records.add(lp);
			sentDbRecords++;
		}
		return records;
	}

	public static List<KeyValue<String,String>> getFilteredOnOffRecords(String lp){
		List<KeyValue<String,String>> records = new ArrayList<KeyValue<String,String>>();
		for( KafkaLineProtocol klp: new KafkaLineProtocol(lp).getKVsFromLineProtocol()){
			String meas = klp.getMeasurement();
			if(msOnOffFilterConfig.keySet().contains(meas)){
				Set<String> sField = msOnOffFilterConfig.get(meas);
				if(sField == null){
					records.add( new KeyValue<String,String>(klp.getKey(),klp.getValue()));
					sentOnOffRecords++;
				} else {
					String field = klp.getFieldName();
					if( sField.contains(field)){
						records.add( new KeyValue<String,String>(klp.getKey(),klp.getValue()));
						sentOnOffRecords++;
					}
				}
			}
		}
		return records;
	}
	
	public static List<KeyValue<String,String>> getFilteredAggregatorRecords(String lp){
		List<KeyValue<String,String>> records = new ArrayList<KeyValue<String,String>>();
		for( KafkaLineProtocol klp: new KafkaLineProtocol(lp).getKVsFromLineProtocol()){
			String meas = klp.getMeasurement();
			if(msAggregatorFilterConfig.keySet().contains(meas)){
				Set<String> sField = msAggregatorFilterConfig.get(meas);
				if(sField == null){
					records.add( new KeyValue<String,String>(klp.getKey(),klp.getValue()));
					sentAggrRecords++;
				} else {
					String field = klp.getFieldName();
					if( sField.contains(field)){
						records.add( new KeyValue<String,String>(klp.getKey(),klp.getValue()));
						sentAggrRecords++;
					}
				}
			}
		}
		return records;
	}

	public static String generateKey( String meas, String field){
		return new String(meas + KEY_SEPARATOR + field).trim();
	}

	public static Map<String,Set<String>> getFilterConfigration( List<Map<String,String>> filterConfig ){
		Map<String, Set<String>> filterConf = new HashMap<String, Set<String>>();
		if( filterConfig == null){
			return filterConf;
		}
		for ( Map<String,String> entry: filterConfig){
			if( ! entry.containsKey(ARGPARSE_SELECTION_MEASUREMENT_KEY)){
				String msg = "Configuration file - Filter section not well written. Miss "+ARGPARSE_SELECTION_MEASUREMENT_KEY+" argument: "+entry;
				logger.error(msg);
				System.exit(1);
			}
			String meas = entry.get(ARGPARSE_SELECTION_MEASUREMENT_KEY);
			String fields = entry.getOrDefault(ARGPARSE_SELECTION_FIELDNAME_KEY, null);
			if(fields == null){
				filterConf.put(meas, null);
			} else {
				Set<String> sFields = new HashSet<String>();
				for(String item: fields.split(",")) sFields.add(item);
				filterConf.put(meas,sFields);
			}
		}
		return filterConf;
	}
	public static void main(String[] args) throws Exception {
		startMs = System.currentTimeMillis(); 
		
        /* Parse command line argumets */
    	ArgumentParser parser = argParser();
        Namespace res = parser.parseArgs(args);
        String config_filename = res.getString(ARGPARSE_CONFIG);
        
        /* Parse yaml configuration file */
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        YamlRouterConfig config = mapper.readValue( new File(config_filename), YamlRouterConfig.class);
		
		/* Logger configuration */
		String log4jfilename = config.getGeneral().get(GENERAL_LOGFILENAME_CONFIG);
		PropertyConfigurator.configure(log4jfilename);
		
		// Kafka configuration section
		Map<String,String> kafka_config = config.getKafka_config();
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, DEFAULT_APPLICATION_ID_CONFIG);
        props.put(StreamsConfig.CLIENT_ID_CONFIG, DEFAULT_CLIENT_ID_CONFIG);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka_config.get(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, DEFAULT_NUM_STREAM_THREADS_CONFIG);
		
		// Component configuration section
		Map<String,String> component_config = config.getComponent_config();
		String input_topic = component_config.get(TOPICS_INPUT_CONFIG);
		String output_database_topic = component_config.get(TOPICS_OUTPUT_DATABASE_CONFIG);
		String output_onoff_topic = component_config.get(TOPICS_OUTPUT_ONOFF_CONFIG);
		String output_aggregator_topic = component_config.get(TOPICS_OUTPUT_AGGREGATOR_CONFIG);
		logger.info("component_config.topics.input: " + input_topic);
		logger.info("component_config.topics.output.database: " + output_database_topic);
		logger.info("component_config.topics.output.onoff: " + output_onoff_topic);
		logger.info("component_config.topics.output.aggregator: " + output_aggregator_topic);
		
		// Filter configuration section
		msDatabaseFilterConfig = getFilterConfigration(config.getDatabase_filter());
		msOnOffFilterConfig    = getFilterConfigration(config.getOnoff_filter());
		msAggregatorFilterConfig = getFilterConfigration(config.getAggregator_filter());
		logger.info("databaseFilterConfig.set: " + msDatabaseFilterConfig.toString());
		logger.info("onOffFilterConfig.set: " + msOnOffFilterConfig.toString());
		logger.info("aggregatorFilterConfig.set: " + msAggregatorFilterConfig.toString());

		// Statistics configuration section
		Map<String,String> statsConfig = config.getStats_config();
		statsEnabled = Boolean.valueOf(statsConfig.getOrDefault("enabled", DEFAULT_STATS_ENABLED));
		logger.info("Stats Enabled?: "+ statsEnabled);
		if( statsEnabled ) {
			try {
				datagramSocket = new DatagramSocket();
			} catch (SocketException e) {
				logger.error("Error while creating UDP socket", e);
			}
			statsType = DEFAULT_STATS_TYPE;
			statsEndpointHostname = statsConfig.getOrDefault("hostname", DEFAULT_STATS_HOSTNAME);
			statsEndpointPort = Integer.parseInt(statsConfig.getOrDefault("port", DEFAULT_STATS_PORT));
			statsPeriodMs = Integer.parseInt(statsConfig.getOrDefault("period_ms", DEFAULT_STATS_PERIOD));
			logger.info("Stats Endpoint Hostname: "+statsEndpointHostname);
			logger.info("Stats Endpoint Port: "+statsEndpointPort);
			logger.info("Stats Period: "+statsPeriodMs+"ms");
			try {
				statsAddress = InetAddress.getByName(statsEndpointHostname);
			} catch (IOException e) {
				logger.error("Error opening creation address using hostname: "+statsEndpointHostname, e);
			}
        }

		final StreamsBuilder builder = new StreamsBuilder();
		KStream<String, String> input_data = builder.stream(input_topic);
		          
		// database Filter
        try {
			input_data.flatMapValues( value -> getFilteredDatabaseRecords(value)).to(output_database_topic);
        } catch (Exception e) {
        	e.printStackTrace();
		}

		// onOff Filter
        try {
			input_data.flatMap( (key,value) -> getFilteredOnOffRecords(value)).to(output_onoff_topic);
        } catch (Exception e) {
        	e.printStackTrace();
		}

		// aggregator Filter
        try {
			input_data.flatMap( (key,value) -> getFilteredAggregatorRecords(value)).to(output_aggregator_topic);
        } catch (Exception e) {
        	e.printStackTrace();
		}
        
		final Topology topology = builder.build();
        logger.info(topology.describe().toString());
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(10);
 
        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread(THREAD_NAME) {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });
 
        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
	}

	static void stats() {
		if(!statsEnabled) return;
		long nowMs = System.currentTimeMillis();
		if ( nowMs - startMs > statsPeriodMs) {
			if(receivedRecords < 0) receivedRecords = 0;
			if(sentDbRecords < 0) sentDbRecords = 0;
			if(sentOnOffRecords < 0) sentOnOffRecords = 0;
			if(sentAggrRecords < 0) sentAggrRecords = 0;
			try{ 
				startMs = nowMs;
				String hostname = InetAddress.getLocalHost().getHostName();
				if(statsType.equals(STATS_TYPE_INFLUXDB)) {
					String data2send = "kafka_streams,application_id="+DEFAULT_APPLICATION_ID_CONFIG+",hostname="+hostname;
					data2send += " receivedRecords="+receivedRecords+"i,sentDatabaseRecords="+sentDbRecords;
					data2send += "i,sentOnOffRecords="+sentOnOffRecords+"i,sentAggregatorRecords="+sentAggrRecords+"i "+nowMs+"000000";
					DatagramPacket packet = new DatagramPacket(data2send.getBytes(), data2send.length(), statsAddress, statsEndpointPort);
					datagramSocket.send(packet);
				}
			} catch (IOException e) {
				logger.warn("Error stat: "+e.getMessage());
			}
		}
	}
    
    private static ArgumentParser argParser() {
        @SuppressWarnings("deprecation")
		ArgumentParser parser = ArgumentParsers
            .newArgumentParser(DEFAULT_APPLICATION_ID_CONFIG)
            .defaultHelp(true)
            .description(DEFAULT_CLIENT_DESCRIPTION);

        parser.addArgument("--config")
    	    .action(store())
            .required(true)
            .type(String.class)
            .dest("config")
            .help("config file");

        return parser;
    }
}