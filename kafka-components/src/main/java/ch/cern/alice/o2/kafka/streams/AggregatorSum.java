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
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.Grouped;

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
import java.time.Duration;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import ch.cern.alice.o2.kafka.utils.LineProtocol;
import ch.cern.alice.o2.kafka.utils.SimplePair;
import ch.cern.alice.o2.kafka.utils.YamlAggregatorConfig;

public class AggregatorSum {
	private static Logger logger = LoggerFactory.getLogger(AggregatorSum.class); 
        
    private static String ARGPARSE_CONFIG = "config";
    private static String GENERAL_LOGFILENAME_CONFIG = "log4jfilename";
    private static String AGGREGATION_WINDOW_S_CONFIG = "window_s";
    private static String AGGREGATION_TOPIC_INPUT_CONFIG = "topic.input";
    private static String AGGREGATION_TOPIC_OUTPUT_CONFIG = "topic.output";

	private static String DEFAULT_NUM_STREAM_THREADS_CONFIG = "1";
	private static String DEFAULT_APPLICATION_ID_CONFIG = "streams-aggregator-avg";
	private static String DEFAULT_CLIENT_ID_CONFIG = "streams-aggregator-avg-client";
	
	private static String THREAD_NAME = "aggregator-sum-shutdown-hook";
    private static String FUNCTION_NAME = "sum";
    
	public static String getFastMeasurement(String meas) {
		char [] temp = new char[50];
		char [] ch_meas = meas.toCharArray(); 
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
	
	public static String tripletsToString(Triplet<String, Double, String> qwe) {
		return new String(qwe.getValue0()+","+qwe.getValue1()+","+qwe.getValue2());
	}
	
	public static List<Triplet<String,Double,String>> getTriplets(LineProtocol lp, Map<String,SimplePair> aggr_conf){
		String meas = lp.getMeasurement();
		if(aggr_conf.containsKey(meas)) {
			String func = aggr_conf.get(meas).key;
			String [] tags2remove = aggr_conf.get(meas).value.split(",");
			return lp.dropTagKeys(tags2remove).dropNotNumberFields().getTriplets(func);
		} else {
			return new ArrayList<Triplet<String,Double,String>>();
		}
	}
	public static String getLineProtocol(Windowed<String> key, Double value, String op) {
		String lp = key.key().replace("|", " ")+"_"+op+"="+value.toString()+" "+key.window().end()+"000000";
		return lp;
	}
	
	public static void main(String[] args) throws Exception {

        /* Parse command line argumets */
    	ArgumentParser parser = argParser();
        Namespace res = parser.parseArgs(args);
        String config_filename = res.getString(ARGPARSE_CONFIG);

        /* Parse yaml configuration file */
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        YamlAggregatorConfig config = mapper.readValue( new File(config_filename), YamlAggregatorConfig.class);
        
        String log4jfilename = config.getGeneral().get(GENERAL_LOGFILENAME_CONFIG);
        PropertyConfigurator.configure(log4jfilename);

        Map<String,String> kafka_config = config.getkafka_config();
        Map<String,String> aggregation_config = config.getAggregation_config();
        String input_topic = aggregation_config.get(AGGREGATION_TOPIC_INPUT_CONFIG);
        String output_topic = aggregation_config.get(AGGREGATION_TOPIC_OUTPUT_CONFIG);
        long window_s = Long.parseLong(aggregation_config.get(AGGREGATION_WINDOW_S_CONFIG));
        long window_ms = window_s * 1000;

    	Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, DEFAULT_APPLICATION_ID_CONFIG);
        props.put(StreamsConfig.CLIENT_ID_CONFIG, DEFAULT_CLIENT_ID_CONFIG);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka_config.get(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG));
        props.put(StreamsConfig.STATE_DIR_CONFIG, kafka_config.get(StreamsConfig.STATE_DIR_CONFIG));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, window_ms);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, DEFAULT_NUM_STREAM_THREADS_CONFIG);
        
        final StreamsBuilder builder = new StreamsBuilder();
        try {
        	KStream<String, Double> sum_data = builder.stream(input_topic, Consumed.with(Serdes.String(), Serdes.Double()));
        	KStream<String, String> sum_aggr_stream = sum_data
            		.groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
    				.windowedBy(TimeWindows.of(Duration.ofSeconds(window_s)))
    				.reduce((v1,v2) -> v1 + v2 )
    				.toStream()
    				.map((key,value) -> new KeyValue<String,String>(key.toString(),getLineProtocol(key,value,FUNCTION_NAME))); 
        	sum_aggr_stream.to(output_topic);
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
    
    private static ArgumentParser argParser() {
        @SuppressWarnings("deprecation")
		ArgumentParser parser = ArgumentParsers
            .newArgumentParser("kafka-stream-aggregator")
            .defaultHelp(true)
            .description("This tool is used to aggregate kafka messages from a specific topic.");
        parser.addArgument("--config")
    	    .action(store())
            .required(true)
            .type(String.class)
            .dest("config")
            .help("config file");

        return parser;
    }
}