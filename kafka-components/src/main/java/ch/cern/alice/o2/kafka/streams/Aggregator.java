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
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig;
import org.apache.kafka.streams.KeyValue;
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
import java.time.Duration;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import ch.cern.alice.o2.kafka.utils.YamlAggregatorConfig;
import ch.cern.alice.o2.kafka.utils.AvgPair;
import ch.cern.alice.o2.kafka.utils.ExtendedSerdes;
import ch.cern.alice.o2.kafka.utils.KafkaLineProtocol;

public class Aggregator {
    private static Logger logger = LoggerFactory.getLogger(Aggregator.class); 
    
    private static String statsEndpointHostname = "";
	private static int statsEndpointPort = 0;
	private static String statsType;
	private static long statsPeriodMs = 0;
	private static InetAddress statsAddress = null;
	private static boolean statsEnabled = false;
	private static DatagramSocket datagramSocket;
	private static long receivedRecords = 0;
	private static long filteredRecords = 0;
	private static long sentRecords = 0;
	private static long startMs = 0;

	private static String KEY_SEPARATOR = "#";
	private static String FIELD_SEPARATOR = ",";
	private static String TAG_SEPARATOR = ",";

	/* Aggregator default configuration */
	private static final long AGGREGATOR_DEFAULT_GRACE_DURATION_S = 10;

	/* ArgParse parameters */
	private static final String ARGPARSE_SELECTION_MEASUREMENT_KEY = "measurement";
	private static final String ARGPARSE_SELECTION_FIELDNAME_KEY = "field.name";
	private static final String ARGPARSE_SELECTION_TAGSREMOVE_KEY = "tags.remove";

    /* Stats parameters */
	private static final String STATS_TYPE_INFLUXDB = "influxdb";
	private static final String DEFAULT_STATS_TYPE = STATS_TYPE_INFLUXDB;
	private static final String DEFAULT_STATS_PERIOD = "10000";
	private static final String DEFAULT_STATS_ENABLED = "false";
	private static final String DEFAULT_STATS_HOSTNAME = "localhost";
	private static final String DEFAULT_STATS_PORT = "8090";

    private static String ARGPARSE_CONFIG = "config";
    private static String GENERAL_LOGFILENAME_CONFIG = "log4jfilename";
    private static String AGGREGATION_WINDOW_S_CONFIG = "window.s";
    private static String AGGREGATION_TOPIC_INPUT_CONFIG = "topic.input";
    private static String AGGREGATION_TOPIC_OUTPUT_CONFIG = "topic.output";

	private static String DEFAULT_NUM_STREAM_THREADS_CONFIG = "1";
	private static String DEFAULT_APPLICATION_ID_CONFIG = "streams-aggregator";
	private static String DEFAULT_CLIENT_ID_CONFIG = "streams-aggregator-client";
    private static String DEFAULT_CLIENT_DESCRIPTION = "This tool is used to aggregate using the avg/sum/min/max functions.";
    
	private static String THREAD_NAME = "aggregator-aggr-shutdown-hook";
	private static String AVG_FUNCTION_NAME = "avg";
	private static String SUM_FUNCTION_NAME = "sum";
	private static String MIN_FUNCTION_NAME = "min";
	private static String MAX_FUNCTION_NAME = "max";
	
	/*
		*  Input Record has this format
		*  String  mfKey   = meas#fieldName
		*  String tvtValue = tags#fieldValue#timestamp
		*
		*  ## Variable description:   
		*  String tags = tagKey1=tagValue1,....,TagKeyN=TagValueN
		*  String timestamp = <optional>
	*/
	public static Boolean filterRecords(String mfKey, String tvtValue, Set<String> allowedFieldMeas){
		return allowedFieldMeas.contains(mfKey);
	}

	public static List<KeyValue<String, Double>> manageTagsAndValue( String mfKey, String tvtValue, Map<String,Set<String>> tagsToRemove){
		stats();
		List<KeyValue<String, Double>> data = new ArrayList<KeyValue<String, Double>>();
		KafkaLineProtocol klp = new KafkaLineProtocol(mfKey, tvtValue);
		klp.removeTags(tagsToRemove.get(mfKey));
		Double doubleValue = klp.getDoublefieldValue();
		if( doubleValue != null){
			KeyValue<String,Double> newKeyValue = new KeyValue<String,Double>(klp.getMeasTagFieldKey(),doubleValue);
			data.add(newKeyValue);
			filteredRecords++;
		}
		return data;
	}

	public static KeyValue<String,String> getLineProtocol(Windowed<String> key, Double value, String op) {
		String lp = key.key().replace("#", " ")+"_"+op+"="+value.toString()+" "+key.window().end()+"000000";
		return new KeyValue<String,String>(key.key(),lp);
	}

	public static Map<String,Set<String>> getTagToRemove( List<Map<String,String>> filterConfig ){
		Map<String,Set<String>> tagsToRemove = new HashMap<String,Set<String>>();
		if( filterConfig != null){
			for ( Map<String,String> entry: filterConfig){
				if( !(entry.containsKey(ARGPARSE_SELECTION_MEASUREMENT_KEY) && (entry.containsKey(ARGPARSE_SELECTION_FIELDNAME_KEY)))){
					String msg = "Selection section of the configuragion file not well written.";
					msg += "Miss "+ARGPARSE_SELECTION_MEASUREMENT_KEY+" and/or "+ARGPARSE_SELECTION_FIELDNAME_KEY+ ": "+entry;
					logger.error(msg);
					System.exit(1);
				}
				String meas = entry.get(ARGPARSE_SELECTION_MEASUREMENT_KEY);
				String [] fields = entry.get(ARGPARSE_SELECTION_FIELDNAME_KEY).split(FIELD_SEPARATOR);
				String tagsRemove = entry.getOrDefault(ARGPARSE_SELECTION_TAGSREMOVE_KEY,"");
				for( String fieldName: fields){
					Set<String> setTagsRemove = new HashSet<String>();
					if( tagsRemove != null){
						for( String tag: tagsRemove.split(TAG_SEPARATOR)){ 
							setTagsRemove.add(tag);
						}
					}
					tagsToRemove.put(generateKey(meas,fieldName),setTagsRemove);
				}
			}
		}
		return tagsToRemove;
	}

	public static String generateKey( String meas, String field){
		return new String(meas + KEY_SEPARATOR + field).trim();
	}
	
	public static void main(String[] args) throws Exception {
        
        /* Parse command line argumets */
    	ArgumentParser parser = argParser();
        Namespace res = parser.parseArgs(args);
        String config_filename = res.getString(ARGPARSE_CONFIG);

        /* Parse yaml configuration file */
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
		YamlAggregatorConfig config = mapper.readValue( new File(config_filename), YamlAggregatorConfig.class);
		
		/* log configuration */
        String log4jfilename = config.getGeneral().get(GENERAL_LOGFILENAME_CONFIG);
		PropertyConfigurator.configure(log4jfilename);

		Map<String,Set<String>> avgTagsToRemove = getTagToRemove(config.getAvg_filter());
		Map<String,Set<String>> sumTagsToRemove = getTagToRemove(config.getSum_filter());
		Map<String,Set<String>> minTagsToRemove = getTagToRemove(config.getMin_filter());
		Map<String,Set<String>> maxTagsToRemove = getTagToRemove(config.getMax_filter());
		
		Set<String> avgAllowedFieldMeas = avgTagsToRemove.keySet();
		Set<String> sumAllowedFieldMeas = sumTagsToRemove.keySet();
		Set<String> minAllowedFieldMeas = minTagsToRemove.keySet();
		Set<String> maxAllowedFieldMeas = maxTagsToRemove.keySet();

		logger.info("avg-filter.meas-field: " + avgAllowedFieldMeas);
		logger.info("avg-filter.'tags to remove': " + avgTagsToRemove);
		logger.info("sum-filter.meas-field: " + sumAllowedFieldMeas);
		logger.info("sum-filter.'tags to remove': " + sumTagsToRemove);
		logger.info("min-filter.meas-field: " + minAllowedFieldMeas);
		logger.info("min-filter.'tags to remove': " + minTagsToRemove);
		logger.info("max-filter.meas-field: " + maxAllowedFieldMeas);
		logger.info("max-filter.'tags to remove': " + maxTagsToRemove);

		// Component configuration section
		Map<String,String> aggregation_config = config.getAggregation_config();
        String input_topic = aggregation_config.get(AGGREGATION_TOPIC_INPUT_CONFIG);
        String output_topic = aggregation_config.get(AGGREGATION_TOPIC_OUTPUT_CONFIG);
        long window_s = Long.parseLong(aggregation_config.get(AGGREGATION_WINDOW_S_CONFIG));
        long window_ms = window_s * 1000;
        logger.info("aggregation.window.s: " + window_s);
        logger.info("aggregation.topic.input: " + input_topic);
		logger.info("aggregation.topic.output: " + output_topic);

		// Kafka configuration section
		Map<String,String> kafka_config = config.getkafka_config();
		Properties props = new Properties();
		String stateDir = kafka_config.get(StreamsConfig.STATE_DIR_CONFIG);
		File f = new File("/Path/To/File/or/Directory");
		if (!(f.exists() && f.isDirectory())) {
			logger.error("Directory '"+stateDir+"' does not exist. Exit");
			System.exit(1);
		}

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, kafka_config.getOrDefault(StreamsConfig.APPLICATION_ID_CONFIG ,DEFAULT_APPLICATION_ID_CONFIG));
        props.put(StreamsConfig.CLIENT_ID_CONFIG, kafka_config.getOrDefault(StreamsConfig.APPLICATION_ID_CONFIG, DEFAULT_CLIENT_ID_CONFIG));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka_config.get(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG));
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, window_ms);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, kafka_config.getOrDefault(StreamsConfig.NUM_STREAM_THREADS_CONFIG, DEFAULT_NUM_STREAM_THREADS_CONFIG));
		
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
			
		// AVG Function
        try {
			KStream<String, String> avg_aggr_stream = input_data
					.filter( (key,value) -> filterRecords(key,value,avgAllowedFieldMeas))
					.flatMap((key,value) -> manageTagsAndValue(key,value,avgTagsToRemove))
					.mapValues( (key,value) -> new AvgPair(value,1) )
            		.groupByKey(Grouped.with(Serdes.String(), ExtendedSerdes.AvgPair() ))
    				.windowedBy(TimeWindows.of(Duration.ofSeconds(window_s)).grace(Duration.ofSeconds(AGGREGATOR_DEFAULT_GRACE_DURATION_S)))
                    .reduce((v1,v2) -> v1.add(v2) )
                    .suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded()))
    				.toStream()
    				.map((key,value) -> getLineProtocol(key,value.getAverage(),AVG_FUNCTION_NAME)); 
        	avg_aggr_stream.to(output_topic);
        } catch (Exception e) {
        	e.printStackTrace();
		}

		// SUM Function
        try {
			KStream<String, String> sum_aggr_stream = input_data
					.filter( (key,value) -> filterRecords(key,value,sumAllowedFieldMeas))
					.flatMap((key,value) -> manageTagsAndValue(key,value,sumTagsToRemove))
					.groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
    				.windowedBy(TimeWindows.of(Duration.ofSeconds(window_s)).grace(Duration.ofSeconds(AGGREGATOR_DEFAULT_GRACE_DURATION_S)))
                    .reduce((v1,v2) -> v1 + v2 )
                    .suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded()))
    				.toStream()
    				.map((key,value) -> getLineProtocol(key,value,SUM_FUNCTION_NAME)); 
        	sum_aggr_stream.to(output_topic);
        } catch (Exception e) {
        	e.printStackTrace();
		}

		// MIN Function
        try {
			KStream<String, String> min_aggr_stream = input_data
					.filter( (key,value) -> filterRecords(key,value,minAllowedFieldMeas))
					.flatMap((key,value) -> manageTagsAndValue(key,value,minTagsToRemove))
					.groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
    				.windowedBy(TimeWindows.of(Duration.ofSeconds(window_s)).grace(Duration.ofSeconds(AGGREGATOR_DEFAULT_GRACE_DURATION_S)))
                    .reduce((v1,v2) -> v1 < v2 ? v1 : v2 )        
                    .suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded()))
    				.toStream()
    				.map((key,value) -> getLineProtocol(key,value,MIN_FUNCTION_NAME)); 
        	min_aggr_stream.to(output_topic);
        } catch (Exception e) {
        	e.printStackTrace();
		}

		// MAX Function
        try {
			KStream<String, String> max_aggr_stream = input_data
					.filter( (key,value) -> filterRecords(key,value,maxAllowedFieldMeas))
					.flatMap((key,value) -> manageTagsAndValue(key,value,maxTagsToRemove))
					.groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
    				.windowedBy(TimeWindows.of(Duration.ofSeconds(window_s)).grace(Duration.ofSeconds(AGGREGATOR_DEFAULT_GRACE_DURATION_S)))
                    .reduce((v1,v2) -> v1 > v2 ? v1 : v2 )
                    .suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded()))
    				.toStream()
    				.map((key,value) -> getLineProtocol(key,value,MAX_FUNCTION_NAME)); 
        	max_aggr_stream.to(output_topic);
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
            e.printStackTrace();
            logger.error(e.getMessage(),e);
            System.exit(1);
        }
        System.exit(0);
    }
	
	static void stats() {
		if (statsEnabled){
			final long nowMs = System.currentTimeMillis();
			if(receivedRecords < 0) receivedRecords = 0;
			if(sentRecords < 0) sentRecords = 0;
			if ( nowMs - startMs > statsPeriodMs) {
				try{ 
					startMs = nowMs;
					final String hostname = InetAddress.getLocalHost().getHostName();
					if(statsType.equals(STATS_TYPE_INFLUXDB)) {
						String data2send = "kafka_streams,application_id="+DEFAULT_APPLICATION_ID_CONFIG+",hostname="+hostname;
						data2send += " receivedRecords="+receivedRecords+"i,filteredRecords="+filteredRecords+"i,sentRecords="+sentRecords+"i "+nowMs+"000000";
						final DatagramPacket packet = new DatagramPacket(data2send.getBytes(), data2send.length(), statsAddress, statsEndpointPort);
						datagramSocket.send(packet);
					} 
				} catch (final IOException e) {
					logger.warn("Error stat: "+e.getMessage());
				
				}
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