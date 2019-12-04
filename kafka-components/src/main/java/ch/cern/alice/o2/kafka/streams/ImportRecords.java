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
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
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
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import ch.cern.alice.o2.kafka.utils.KafkaLineProtocol;
import ch.cern.alice.o2.kafka.utils.YamlImportRecordsConfig;

public final class ImportRecords {
	private static String statsEndpointHostname = "";
	private static int statsEndpointPort = 0;
	private static String statsType;
	private static long statsPeriodMs = 0;
	private static InetAddress statsAddress = null;
	private static boolean statsEnabled = false;
	private static DatagramSocket datagramSocket;
	private static long receivedRecords = 0;
	private static long sentRecords = 0;
	private static long startMs = 0;
	private static String input_topic = "";
	private static String output_topic = "";

	private static Logger logger = LoggerFactory.getLogger(ImportRecords.class); 
    private static String ARGPARSE_CONFIG = "config";
    private static String GENERAL_LOGFILENAME_CONFIG = "log4jfilename";
    private static String TOPICS_INPUT_CONFIG = "topic.input";
	private static String TOPICS_OUTPUT_CONFIG = "topic.output";
	
	/* Process components' name */
	private static String SOURCE_PROCESSOR_NAME = "sourceProcessorComponent";
	private static String IMPORT_PROCESSOR_NAME = "importProcessorComponent";
	private static String SINK_PROCESSOR_NAME = "sinkProcessorComponent";
	
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

	static class importProcessorSupplier implements ProcessorSupplier<String, String> {
		@Override
		public Processor<String,String> get(){
			return new Processor<String,String>(){
				private ProcessorContext context;
			
				@Override
				//@SuppressWarnings("unchecked")
				public void init( final ProcessorContext context){
					this.context = context;	
				}
				
				@Override
				public void process(String key, String lp) {
					//System.out.println("\n\t\t\t topic: "+input_topic+" -> ('key','value') ('"+ key+"','"+lp+"')");
					receivedRecords++;
					if( statsEnabled ) {
						try {
							stats();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
					for( KafkaLineProtocol klp: new KafkaLineProtocol(lp).getKVsFromLineProtocol()){
						String k = klp.getKey().trim();
						String v = klp.getValue().trim();
						context.forward(k,v);
						sentRecords++;
						//System.out.println("\t\t\t ('key','value') ('"+klp.getKey()+"','"+klp.getValue()+"') -> topic: "+output_topic+"");
					}
				}
	  
				@Override
				public void close() {}
  			};
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
        YamlImportRecordsConfig config = mapper.readValue( new File(config_filename), YamlImportRecordsConfig.class);
		
		/* Logger configuration */
		String log4jfilename = config.getGeneral().get(GENERAL_LOGFILENAME_CONFIG);
		PropertyConfigurator.configure(log4jfilename);
		
		Map<String,String> import_config = config.getImport_config();
		Map<String,String> statsConfig = config.getStats_config();

		input_topic = import_config.get(TOPICS_INPUT_CONFIG);
		output_topic = import_config.get(TOPICS_OUTPUT_CONFIG);
		logger.info("import_config.topics.input: " + input_topic);
		logger.info("import_config.topics.output: " + output_topic);
		
		statsEnabled = Boolean.valueOf(statsConfig.getOrDefault("enabled", DEFAULT_STATS_ENABLED));
        statsType = DEFAULT_STATS_TYPE;
        statsEndpointHostname = statsConfig.getOrDefault("hostname", DEFAULT_STATS_HOSTNAME);
        statsEndpointPort = Integer.parseInt(statsConfig.getOrDefault("port", DEFAULT_STATS_PORT));
        statsPeriodMs = Integer.parseInt(statsConfig.getOrDefault("period_ms", DEFAULT_STATS_PERIOD));
		logger.info("Stats Enabled?: "+ statsEnabled);
		
		try {
			datagramSocket = new DatagramSocket();
		} catch (SocketException e) {
			logger.error("Error while creating UDP socket", e);
		}
		
		if( statsEnabled ) {
			logger.info("Stats Endpoint Hostname: "+statsEndpointHostname);
			logger.info("Stats Endpoint Port: "+statsEndpointPort);
			logger.info("Stats Period: "+statsPeriodMs+"ms");
			try {
				statsAddress = InetAddress.getByName(stats_endpoint_hostname);
			} catch (IOException e) {
				logger.error("Error opening creation address using hostname: "+stats_endpoint_hostname, e);
			}
        }

		Map<String,String> kafka_config = config.getKafka_config();
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, DEFAULT_APPLICATION_ID_CONFIG);
        props.put(StreamsConfig.CLIENT_ID_CONFIG, DEFAULT_CLIENT_ID_CONFIG);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka_config.get(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, DEFAULT_NUM_STREAM_THREADS_CONFIG);
		
		Topology builder = new Topology();

		// add the source processor node that takes Kafka topic "source-topic" as input
		builder.addSource(SOURCE_PROCESSOR_NAME, input_topic)

				// add the FilterProcessorSupplier node which takes records from the source processor and filters them
				.addProcessor(IMPORT_PROCESSOR_NAME,  new importProcessorSupplier(), SOURCE_PROCESSOR_NAME)

				// add the sink processor node that export data to the output_topic
				.addSink(SINK_PROCESSOR_NAME, output_topic, IMPORT_PROCESSOR_NAME);

        
		// generating the topology
		logger.info(builder.describe().toString());

		// constructing a streams client with the properties and topology
        final KafkaStreams streams = new KafkaStreams(builder, props);
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
	
	static void stats() throws IOException {
		long nowMs = System.currentTimeMillis();
		if(receivedRecords < 0) receivedRecords = 0;
		if(sentRecords < 0) sentRecords = 0;
    	if ( nowMs - startMs > statsPeriodMs) {
			startMs = nowMs;
    	    String hostname = InetAddress.getLocalHost().getHostName();
			if(statsType.equals(STATS_TYPE_INFLUXDB)) {
				String data2send = "kafka_streams,application_id="+DEFAULT_APPLICATION_ID_CONFIG+",hostname="+hostname;
				data2send += " receivedRecords="+receivedRecords+"i,sentRecords="+sentRecords+"i "+nowMs+"000000";
				DatagramPacket packet = new DatagramPacket(data2send.getBytes(), data2send.length(), statsAddress, statsEndpointPort);
				datagramSocket.send(packet);
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