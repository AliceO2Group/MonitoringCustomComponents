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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.KeyValue;

import org.apache.log4j.PropertyConfigurator;
import org.javatuples.Triplet;
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
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.HashMap;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import ch.cern.alice.o2.kafka.utils.YamlDispatcherConfig;
import ch.cern.alice.o2.kafka.utils.LineProtocol;
import ch.cern.alice.o2.kafka.utils.SimplePair;

public class ChangeDetector {
	private static String version = "0.0.1";
	/*
	Version 0.0.1 first version
	*/

	private static int stats_endpoint_port = 0;
	private static String stats_type;
	private static long stats_period_ms = 0;
	private static InetAddress stats_address = null;
	private static DatagramSocket datagramSocket;
	private static long receivedRecords = 0;
	private static long sentRecords = 0;
	private static long startMs = 0;

	private static Map<String,String> table = new HashMap<String,String>();
	private static Set<String> allowed_meas = new HashSet<String>();


	private static Logger logger = LoggerFactory.getLogger(ChangeDetector.class); 
    private static String ARGPARSE_CONFIG = "config";
    private static String GENERAL_LOGFILENAME_CONFIG = "log4jfilename";
    private static String TOPICS_INPUT_CONFIG = "topic.input";
	private static String TOPICS_OUTPUT_CONFIG = "topic.output";
	private static String REFRESH_PERIOD_S_CONFIG = "refresh.period.s";
	private static String FILTER_MEASUREMENT_CONFIG = "filter.measurements";

	/* Stats parameters */
	private static final String STATS_TYPE_INFLUXDB = "influxdb";
	private static final String DEFAULT_STATS_TYPE = STATS_TYPE_INFLUXDB;
	private static final String DEFAULT_STATS_PERIOD = "10000";
	private static final String DEFAULT_STATS_ENABLED = "false";
	private static final String DEFAULT_STATS_HOSTNAME = "localhost";
	private static final String DEFAULT_STATS_PORT = "8090";

	private static String DEFAULT_NUM_STREAM_THREADS_CONFIG = "1";
	private static String DEFAULT_APPLICATION_ID_CONFIG = "streams-app-change-detector";
	private static String DEFAULT_CLIENT_ID_CONFIG = "streams-client-change-detector";

	private static String THREAD_NAME = "change-detector-shutdown-hook";

	public static String getFastMeasurement(String lp) {
		char[] temp = new char[50];
		char[] ch_meas = lp.toCharArray(); 
		for(int i=0; i<ch_meas.length; i++){
			if(ch_meas[i] != ',' ) {
				temp[i] = ch_meas[i];
			} else {
				temp[i] = 0;
				break;
			}
		}
		return new String(temp);
	}
	
	public static boolean filterMeas(String lp){
		String meas = getFastMeasurement(lp);
		return allowed_meas.contains(meas);
	}

	public static List<String> getSingleton(String meas){

	}

	public static String detectChanges(String meas){

	}


	public static String tripletsToString(Triplet<String, Double, String> t) {
		return new String(t.getValue0()+","+t.getValue1()+","+t.getValue2());
	}
	
	public static List<Triplet<String,Double,String>> getTriplets(LineProtocol lp, Map<String,SimplePair> aggr_conf){
		String meas = lp.getMeasurement();
		if(aggr_conf.containsKey(meas)) {
			String func = aggr_conf.get(meas).key;
			String [] tags2remove = aggr_conf.get(meas).value.split(",");
			return lp.dropTagKeys(tags2remove).dropNotNumberFields().getTriplets(func);
		} else {
			/* The measurement must not be aggregated */
			List<Triplet<String,Double,String>> l = new ArrayList<Triplet<String,Double,String>>();
			l.add(new Triplet<String,Double,String>(lp.toLineProtocol(TimeUnit.NANOSECONDS),new Double(0),""));
			return l;
		}
	}
	
	public static void main(String[] args) throws Exception {
		String stats_endpoint_hostname = null;
		startMs = System.currentTimeMillis(); 
		
        /* Parse command line argumets */
    	ArgumentParser parser = argParser();
        Namespace res = parser.parseArgs(args);
        String config_filename = res.getString(ARGPARSE_CONFIG);
        
        /* Parse yaml configuration file */
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        YamlChangeDetectorConfig config = mapper.readValue( new File(config_filename), YamlChangeDetectorConfig.class);
		
		/* Logger configuration */
		String log4jfilename = config.getGeneral().get(GENERAL_LOGFILENAME_CONFIG);
        PropertyConfigurator.configure(log4jfilename);

		Map<String,String> kafka_consumer_config = config.getKafka_consumer_config();
        Map<String,String> detector = config.getDetector();
        Map<String,String> stats_config = config.getStats_config();

		String input_topic = detector.get(TOPICS_INPUT_CONFIG);
		String output_topic = detector.get(TOPICS_OUTPUT_CONFIG);
		String refresh_period_s = detector.get(REFRESH_PERIOD_S_CONFIG);
		String filter_measurements = detector.get(FILTER_MEASUREMENT_CONFIG);
		
		logger.info("detector.topics.input: " + input_topic);
		logger.info("detector.topics.output: " + output_topic);
		logger.info("detector.refresh.period.s: " + refresh_period_s);
		logger.info("detector.filter.measurements: " + filter_measurements);

		boolean stats_enabled = Boolean.valueOf(stats_config.getOrDefault("enabled", DEFAULT_STATS_ENABLED));
        stats_type = DEFAULT_STATS_TYPE;
        stats_endpoint_hostname = stats_config.getOrDefault("hostname", DEFAULT_STATS_HOSTNAME);
        stats_endpoint_port = Integer.parseInt(stats_config.getOrDefault("port", DEFAULT_STATS_PORT));
        stats_period_ms = Integer.parseInt(stats_config.getOrDefault("period_ms", DEFAULT_STATS_PERIOD));
		logger.info("Stats Enabled?: "+ stats_enabled);
		
		try {
			datagramSocket = new DatagramSocket();
		} catch (SocketException e) {
			logger.error("Error while creating UDP socket", e);
		}
		
		if( stats_enabled ) {
			logger.info("Stats Endpoint Hostname: "+stats_endpoint_hostname);
			logger.info("Stats Endpoint Port: "+stats_endpoint_port);
			logger.info("Stats Period: "+stats_period_ms+"ms");
			try {
				stats_address = InetAddress.getByName(stats_endpoint_hostname);
			} catch (IOException e) {
				logger.error("Error opening creation address using hostname: "+stats_endpoint_hostname, e);
			}
        }



        Map<String,String> kafka_config = config.getkafka_config();
                
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, DEFAULT_APPLICATION_ID_CONFIG);
        props.put(StreamsConfig.CLIENT_ID_CONFIG, DEFAULT_CLIENT_ID_CONFIG);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka_config.get(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, DEFAULT_NUM_STREAM_THREADS_CONFIG);
        
        final StreamsBuilder builder = new StreamsBuilder();
        try {
			KStream<String, String> source = builder.stream(input_topic);
			KStream<String, String> filtered_data = source.filter((key, value) -> filterMeas(value));
			KStream<String, String> singlized_data = filtered_data.flatMapValues(value -> getSingleton(value));
			KStream<String, String> detected_changes = singlized_data.mapValues(value -> detectChanges(value));
			detected_changes.to(output_topic);
	    } catch (Exception e) {
        	e.printStackTrace();
        }
             		      
        final Topology topology = builder.build();
		logger.info(topology.describe().toString());
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);
 
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
	
	private static void stats() throws IOException {
		long nowMs = System.currentTimeMillis();
		if(receivedRecords < 0) receivedRecords = 0;
		if(sentRecords < 0) sentRecords = 0;
    	if ( nowMs - startMs > stats_period_ms) {
			startMs = nowMs;
    	    String hostname = InetAddress.getLocalHost().getHostName();
			if(stats_type.equals(STATS_TYPE_INFLUXDB)) {
				String data2send = "kafka_consumer,endpoint_type=InfluxDB,endpoint="+data_endpoint_hostname+":"+data_endpoint_port_str.replace(',','|')+",hostname="+hostname+",topic="+topicName;
				data2send += " receivedRecords="+receivedRecords+"i,sentRecords="+sentRecords+"i "+nowMs+"000000";
				DatagramPacket packet = new DatagramPacket(data2send.getBytes(), data2send.length(), stats_address, stats_endpoint_port);
				datagramSocket.send(packet);
			}
    	}
	}
    
    private static ArgumentParser argParser() {
        @SuppressWarnings("deprecation")
		ArgumentParser parser = ArgumentParsers
            .newArgumentParser("kafka-stream-dispatcher")
            .defaultHelp(true)
            .description("This tool is used to dispatch kafka messages among topics.");

        parser.addArgument("--config")
    	    .action(store())
            .required(true)
            .type(String.class)
            .dest("config")
            .help("config file");

        return parser;
    }
}