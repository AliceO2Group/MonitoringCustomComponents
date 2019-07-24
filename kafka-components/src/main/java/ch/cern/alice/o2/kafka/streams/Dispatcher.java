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
import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import ch.cern.alice.o2.kafka.utils.YamlDispatcherConfig;
import ch.cern.alice.o2.kafka.utils.LineProtocol;
import ch.cern.alice.o2.kafka.utils.SimplePair;

public class Dispatcher {
	private static Logger logger = LoggerFactory.getLogger(Dispatcher.class); 
    
    private static String ARGPARSE_CONFIG = "config";
    private static String GENERAL_LOGFILENAME_CONFIG = "log4jfilename";
    private static String TOPICS_INPUT_CONFIG = "topic.input";
    private static String TOPICS_OUTPUT_AVG_CONFIG = "topic.avg";
	private static String TOPICS_OUTPUT_SUM_CONFIG = "topic.sum";
	private static String TOPICS_OUTPUT_MIN_CONFIG = "topic.min";
	private static String TOPICS_OUTPUT_MAX_CONFIG = "topic.max";
	private static String TOPICS_OUTPUT_DEFAULT_CONFIG = "topic.default";
	private static String SELECTION_MEASUREMENT_CONFIG = "measurement";
	private static String SELECTION_TAG_TO_REMOVE_CONFIG = "removetags";

	private static String DEFAULT_NUM_STREAM_THREADS_CONFIG = "1";
	private static String DEFAULT_APPLICATION_ID_CONFIG = "streams-dispatcher";
	private static String DEFAULT_CLIENT_ID_CONFIG = "streams-dispatcher-client";

	private static String FUNCTION_AVG = "avg";
	private static String FUNCTION_SUM = "sum";
	private static String FUNCTION_MIN = "min";
	private static String FUNCTION_MAX = "max";

	private static String THREAD_NAME = "dispatcher-shutdown-hook";

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
    	
        /* Parse command line argumets */
    	ArgumentParser parser = argParser();
        Namespace res = parser.parseArgs(args);
        String config_filename = res.getString(ARGPARSE_CONFIG);
        
        /* Parse yaml configuration file */
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        YamlDispatcherConfig config = mapper.readValue( new File(config_filename), YamlDispatcherConfig.class);
		
		/* Logger configuration */
		String log4jfilename = config.getGeneral().get(GENERAL_LOGFILENAME_CONFIG);
        PropertyConfigurator.configure(log4jfilename);

		Map<String,String> topics = config.getTopics();
		String input_topic = topics.get(TOPICS_INPUT_CONFIG);
		String output_avg_topic = topics.get(TOPICS_OUTPUT_AVG_CONFIG);
		String output_sum_topic = topics.get(TOPICS_OUTPUT_SUM_CONFIG);
		String output_min_topic = topics.get(TOPICS_OUTPUT_MIN_CONFIG);
		String output_max_topic = topics.get(TOPICS_OUTPUT_MAX_CONFIG);
		String output_default_topic = topics.get(TOPICS_OUTPUT_DEFAULT_CONFIG);

		logger.info("topic.input: " + input_topic);
		logger.info("topic.output.avg: " + output_avg_topic);
		logger.info("topic.output.sum: " + output_sum_topic);
		logger.info("topic.output.min: " + output_min_topic);
		logger.info("topic.output.max: " + output_max_topic);
		logger.info("topic.output.default: " + output_default_topic);


        Map<String,String> kafka_config = config.getkafka_config();
        Map<String,Map<String,String>[]> aggregators = config.getSelection();
        
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, DEFAULT_APPLICATION_ID_CONFIG);
        props.put(StreamsConfig.CLIENT_ID_CONFIG, DEFAULT_CLIENT_ID_CONFIG);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka_config.get(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, DEFAULT_NUM_STREAM_THREADS_CONFIG);
        
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Double> doubleSerde = Serdes.Double();
        
        Map<String,SimplePair> aggr_conf = new HashMap<String,SimplePair>();
        String meas = null;
        String func = null;
        String tags2rem = null;
        
        // {measurement: (func,tags2remove)}
        for(Map.Entry<String, Map<String,String>[]> entry: aggregators.entrySet()) {
        	for(Map<String,String> item: entry.getValue()) {
        		meas = item.get(SELECTION_MEASUREMENT_CONFIG);
        		func = entry.getKey();
        		tags2rem = item.getOrDefault(SELECTION_TAG_TO_REMOVE_CONFIG, "").replaceAll(" ", "");
        		aggr_conf.put(meas, new SimplePair(func,tags2rem));
        	}
        }
		
		// Print the aggregation configuration
        for(Map.Entry<String, SimplePair> item: aggr_conf.entrySet()) {
        	System.out.println(item.getKey() + " = " + item.getValue());
        }
        
        final StreamsBuilder builder = new StreamsBuilder();
        try {
<<<<<<< HEAD
			KStream<String, String> source = builder.stream(input_topic);

			KStream<String,LineProtocol> lp_data = source.mapValues( 
=======
	        KStream<String, String> source = builder.stream(input_topic);
			
	        KStream<String,LineProtocol> lp_data = source.mapValues( 
>>>>>>> 55f3fc219444a541471bf5c4e5f56f8f23b2ee0b
	        		value -> {
	        			LineProtocol result = new LineProtocol();
						try {
							result = new LineProtocol().fromLineProtocol(value);
						} catch (Exception e1) {
							e1.printStackTrace();
						}
						return result;
					});
			
	        KStream<String,Triplet<String, Double, String>> triplets_data = lp_data
	        		.flatMapValues(value -> getTriplets(value,aggr_conf));
	        
			//triplets_data.mapValues( value -> tripletsToString(value)).to("gra3");

			@SuppressWarnings("unchecked")
			KStream<String,Triplet<String, Double, String>> branches[] = triplets_data.branch(
<<<<<<< HEAD
	        		(key,value) -> value.getValue2().equals(TOPICS_OUTPUT_AVG_CONFIG),
	        		(key,value) -> value.getValue2().equals(TOPICS_OUTPUT_SUM_CONFIG),
	        		(key,value) -> value.getValue2().equals(TOPICS_OUTPUT_MIN_CONFIG),
					(key,value) -> value.getValue2().equals(TOPICS_OUTPUT_MAX_CONFIG),
=======
	        		(key,value) -> value.getValue2().equals(FUNCTION_AVG),
	        		(key,value) -> value.getValue2().equals(FUNCTION_SUM),
	        		(key,value) -> value.getValue2().equals(FUNCTION_MIN),
					(key,value) -> value.getValue2().equals(FUNCTION_MAX),
>>>>>>> 55f3fc219444a541471bf5c4e5f56f8f23b2ee0b
					(key,value) -> true
	        		);
	        
	        branches[0]
	        		.map((key,value) -> new KeyValue<String,Double>(value.getValue0(),value.getValue1())) 
	        		.to(output_avg_topic, Produced.with(stringSerde, doubleSerde));
	        branches[1]
	        		.map((key,value) -> new KeyValue<String,Double>(value.getValue0(),value.getValue1()))
	        		.to(output_sum_topic, Produced.with(stringSerde, doubleSerde));
	        branches[2]
	        		.map((key,value) -> new KeyValue<String,Double>(value.getValue0(),value.getValue1()))
	        		.to(output_min_topic, Produced.with(stringSerde, doubleSerde));
	        branches[3]
	        		.map((key,value) -> new KeyValue<String,Double>(value.getValue0(),value.getValue1()))
	        		.to(output_max_topic, Produced.with(stringSerde, doubleSerde));
			branches[4]
					.mapValues(value -> value.getValue0())
					.to(output_default_topic);
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