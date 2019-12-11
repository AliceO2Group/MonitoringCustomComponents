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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
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
import java.util.Set;
import java.util.HashSet;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import ch.cern.alice.o2.kafka.utils.KafkaLineProtocol;
import ch.cern.alice.o2.kafka.utils.YamlOnOffConfig;

public final class OnOff {
	private static String statsEndpointHostname = "";
	private static int statsEndpointPort = 0;
	private static String statsType;
	private static long statsPeriodMs = 0;
	private static InetAddress statsAddress = null;
	private static boolean statsEnabled = false;
	private static DatagramSocket datagramSocket;
	private static long receivedRecords = 0;
	private static long filteredRecords = 0;
	private static long sentPeriodicRecords = 0;
	private static long sentRecords = 0;
	private static long startMs = 0;
	private static int refresh_period_s = 0;
	private static Set<String> allowedFieldMeas = new HashSet<String>();
	private static Set<String> allowedMeas = new HashSet<String>();

	private static Logger logger = LoggerFactory.getLogger(OnOff.class); 
    private static String ARGPARSE_CONFIG = "config";
    private static String GENERAL_LOGFILENAME_CONFIG = "log4jfilename";
    private static String TOPICS_INPUT_CONFIG = "topic.input";
	private static String TOPICS_OUTPUT_CONFIG = "topic.output";
	private static String REFRESH_PERIOD_S_CONFIG = "refresh.period.s";

	/* Process components' name */
	private static String CHANGELOG_STORE_NAME = "changeLogStore5";
	private static String SOURCE_PROCESSOR_NAME = "sourceProcessorComponent";
	private static String FILTER_PROCESSOR_NAME = "FilterProcessorComponent";
	private static String CHANGELOG_PROCESSOR_NAME = "ChangeLogProcessorComponent";
	private static String SINK_PROCESSOR_NAME = "sinkProcessorComponent";
	
	/* Stats parameters */
	private static final String STATS_TYPE_INFLUXDB = "influxdb";
	private static final String DEFAULT_STATS_TYPE = STATS_TYPE_INFLUXDB;
	private static final String DEFAULT_STATS_PERIOD = "10000";
	private static final String DEFAULT_STATS_ENABLED = "false";
	private static final String DEFAULT_STATS_HOSTNAME = "localhost";
	private static final String DEFAULT_STATS_PORT = "8090";

	private static String DEFAULT_REPLICATION_FACTOR="3";
	private static String DEFAULT_NUM_STREAM_THREADS_CONFIG = "1";
	private static String DEFAULT_APPLICATION_ID_CONFIG = "streams-app-change-detector2";
	private static String DEFAULT_CLIENT_ID_CONFIG = "streams-client-change-detector2";
	private static String DEFAULT_CLIENT_DESCRIPTION = "This tool is used to detect changes in selected metric values.";

	private static String THREAD_NAME = "change-detector-shutdown-hook2";

	static class FilterProcessorSupplier implements ProcessorSupplier<String, String> {
		/*
		*  Input Record has this format
		*  String mfKey    = meas#fieldName
		*  String tvtValue = tags#fieldValue#timestamp
		*
		*  ## Variable description:   
		*  String tags = tagKey1=tagValue1,....,TagKeyN=TagValueN
		*  String timestamp = <optional>
		*/
		
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
				public void process(final String mfKey, final String tvtValue) {
					receivedRecords++;
					if( statsEnabled ) {
						try {
							stats();
						} catch (final IOException e) {
							logger.warn(e.getMessage());
						}
					}
					if( allowedFieldMeas.contains(mfKey)){
						context.forward(mfKey, tvtValue);
						filteredRecords++;
					}
				}
	  
				@Override
				public void close() {}
  			};
		}
	}

	static String getLineProtocolFromEntryStateStore(final KeyValue<String, String> entry, final long timestamp) throws Exception {
		/* 
		 *  Data retrieved from State store has this format:
		 *  String key = measName,tagKey1=tagValue1,....,TagKeyN=TagValueN#fieldName
		 *  String value = fieldValue
		 */

		final String [] vett = entry.key.split("#");
		if(vett.length != 2){
			throw new Exception("Read key from store is not well written: " + entry.key);
		}
		final String lp_key = vett[0];
		final String fieldName = vett[1];
		final String lp = lp_key + " " + fieldName + "=" + entry.value + " " + timestamp + "000000";
		return lp;							
	}

	static class changeLogProcessorSupplier implements ProcessorSupplier<String, String> {
		/*
		*  Input Record has this format
		*  String  mfKey   = meas#fieldName
		*  String tvtValue = tags#fieldValue#timestamp
		*
		*  ## Variable description:   
		*  String tags = tagKey1=tagValue1,....,TagKeyN=TagValueN
		*  String timestamp = <optional>
		*/
		@Override
		public Processor<String,String> get(){
			return new Processor<String,String>(){
				private ProcessorContext context;
				private KeyValueStore<String, String> kvStore;
				
				@Override
				@SuppressWarnings("unchecked")
				public void init(final ProcessorContext context) {
					this.context = context;
					this.kvStore = (KeyValueStore<String,String>) context.getStateStore(CHANGELOG_STORE_NAME);
					this.context.schedule(Duration.ofSeconds(refresh_period_s), PunctuationType.WALL_CLOCK_TIME, (timestamp) -> {
						final KeyValueIterator<String, String> iter = this.kvStore.all();
						while (iter.hasNext()) {
							final KeyValue<String, String> entry = iter.next();
							try{
								final String lp = getLineProtocolFromEntryStateStore(entry, timestamp);
								context.forward(entry.key, lp);
								sentPeriodicRecords++;
							} catch (final Exception e) {
								logger.warn(e.getMessage());
							}
						}
						iter.close();
						// commit the current processing progress
						context.commit();
					});
				}
			
				@Override
				public void process(final String mfKey, final String tvtValue) {
					final KafkaLineProtocol klp = new KafkaLineProtocol(mfKey,tvtValue);
					final String mtfKey = klp.getMeasTagFieldKey();
					final String fieldValue = klp.getFieldValue();
					final String storeValue = kvStore.get(mtfKey);
					if(storeValue == null){
						final String lp = klp.getLineProtocol();
						logger.debug("kvStore does not contain: "+lp);
						this.kvStore.put(mtfKey,fieldValue);
						context.forward(mfKey, lp);
						sentRecords++;
						context.commit();
					} else {
						if(! fieldValue.equals(storeValue)){
							final String lp = klp.getLineProtocol();
							logger.debug("Change detected. lp: "+lp+" old value: "+storeValue);
							this.kvStore.put(mtfKey,fieldValue);
							context.forward(mfKey, lp);
							context.commit();
							sentRecords++;
						} else {
							logger.debug("NO changes detected.");
						}
					}
				}
			
				@Override
				public void close() {}
			};
		}
	}
	
	public static void main(final String[] args) throws Exception {
		startMs = System.currentTimeMillis(); 
		
        /* Parse command line argumets */
    	final ArgumentParser parser = argParser();
        final Namespace res = parser.parseArgs(args);
        final String config_filename = res.getString(ARGPARSE_CONFIG);
        
        /* Parse yaml configuration file */
        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        final YamlOnOffConfig config = mapper.readValue( new File(config_filename), YamlOnOffConfig.class);
		
		/* Logger configuration */
		final String log4jfilename = config.getGeneral().get(GENERAL_LOGFILENAME_CONFIG);
		PropertyConfigurator.configure(log4jfilename);
		
		final Map<String,String> detector = config.getDetector();
		final Map<String,String> filterConfig = config.getFilter();
        final Map<String,String> statsConfig = config.getStats_config();

		final String input_topic = detector.get(TOPICS_INPUT_CONFIG);
		final String output_topic = detector.get(TOPICS_OUTPUT_CONFIG);
		refresh_period_s = Integer.parseInt(detector.get(REFRESH_PERIOD_S_CONFIG));
		allowedMeas = filterConfig.keySet();
		for (final Map.Entry<String, String> entry : filterConfig.entrySet()) {
			final String meas = entry.getKey();
			final String fields = entry.getValue();
			final String [] fieldsVett = fields.split(",");
			for(final String field: fieldsVett){
				allowedFieldMeas.add(meas+"#"+field);
			}
		} 
		
		logger.info("detector.topics.input: " + input_topic);
		logger.info("detector.topics.output: " + output_topic);
		logger.info("detector.refresh.period.s: " + refresh_period_s);
		logger.info("filter.measurements: " + allowedMeas);
		logger.info("filter.field_measurements: " + allowedFieldMeas);

		statsEnabled = Boolean.valueOf(statsConfig.getOrDefault("enabled", DEFAULT_STATS_ENABLED));
        statsType = DEFAULT_STATS_TYPE;
        statsEndpointHostname = statsConfig.getOrDefault("hostname", DEFAULT_STATS_HOSTNAME);
        statsEndpointPort = Integer.parseInt(statsConfig.getOrDefault("port", DEFAULT_STATS_PORT));
        statsPeriodMs = Integer.parseInt(statsConfig.getOrDefault("period_ms", DEFAULT_STATS_PERIOD));
		logger.info("Stats Enabled?: "+ statsEnabled);
		
		try {
			datagramSocket = new DatagramSocket();
		} catch (final SocketException e) {
			logger.error("Error while creating UDP socket", e);
		}
		
		if( statsEnabled ) {
			logger.info("Stats Endpoint Hostname: "+statsEndpointHostname);
			logger.info("Stats Endpoint Port: "+statsEndpointPort);
			logger.info("Stats Period: "+statsPeriodMs+"ms");
			try {
				statsAddress = InetAddress.getByName(statsEndpointHostname);
			} catch (final IOException e) {
				logger.error("Error opening creation address using hostname: "+statsEndpointHostname, e);
			}
        }

		final Map<String,String> kafka_config = config.getKafka_config();
		final Properties props = new Properties();
		props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, DEFAULT_REPLICATION_FACTOR);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, DEFAULT_APPLICATION_ID_CONFIG);
        props.put(StreamsConfig.CLIENT_ID_CONFIG, DEFAULT_CLIENT_ID_CONFIG);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka_config.get(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, DEFAULT_NUM_STREAM_THREADS_CONFIG);
		
		final StoreBuilder<KeyValueStore<String, String>> kvStore = Stores.keyValueStoreBuilder(
			//Stores.persistentKeyValueStore(CHANGELOG_STORE_NAME),
			Stores.inMemoryKeyValueStore(CHANGELOG_STORE_NAME),
				Serdes.String(),
				Serdes.String())
			.withCachingEnabled();
		
		final Topology builder = new Topology();

		// add the source processor node that takes Kafka topic "source-topic" as input
		builder.addSource(SOURCE_PROCESSOR_NAME, input_topic)

				// add the FilterProcessorSupplier node which takes records from the source processor and filters them
				.addProcessor(FILTER_PROCESSOR_NAME,  new FilterProcessorSupplier(), SOURCE_PROCESSOR_NAME)

				// add the changeLogProcessorSupplier node which takes data from filterProcessor node and evaluates changes
				.addProcessor(CHANGELOG_PROCESSOR_NAME, new changeLogProcessorSupplier(), FILTER_PROCESSOR_NAME)

				// add the change log store associated with the changeLogProcessor node
				.addStateStore(kvStore, CHANGELOG_PROCESSOR_NAME)

				// add the sink processor node that export data to the output_topic
				.addSink(SINK_PROCESSOR_NAME, output_topic, CHANGELOG_PROCESSOR_NAME);

        
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
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
	}
	static void stats() throws IOException {
		final long nowMs = System.currentTimeMillis();
		if(receivedRecords < 0) receivedRecords = 0;
		if(sentRecords < 0) sentRecords = 0;
    	if ( nowMs - startMs > statsPeriodMs) {
			try{ 
				startMs = nowMs;
				final String hostname = InetAddress.getLocalHost().getHostName();
				if(statsType.equals(STATS_TYPE_INFLUXDB)) {
					String data2send = "kafka_streams,application_id="+DEFAULT_APPLICATION_ID_CONFIG+",hostname="+hostname;
					data2send += " receivedRecords="+receivedRecords+"i,filteredRecords="+filteredRecords+"i,sentPeriodicRecords=";
					data2send += sentPeriodicRecords+"i,sentRecords="+sentRecords+"i "+nowMs+"000000";
					final DatagramPacket packet = new DatagramPacket(data2send.getBytes(), data2send.length(), statsAddress, statsEndpointPort);
					datagramSocket.send(packet);
				} 
			} catch (final IOException e) {
				logger.warn("Error stat: "+e.getMessage());
			
			}
    	}
	}
    
    private static ArgumentParser argParser() {
        @SuppressWarnings("deprecation")
		final
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