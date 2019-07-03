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
import org.apache.kafka.streams.kstream.Windowed;


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
	public static String getFastMeasurement(String meas) {
		char[] temp = new char[50];
		char[] ch_meas = meas.toCharArray(); 
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
			return new ArrayList<Triplet<String,Double,String>>();
		}
	}
	public static String getLineProtocol(Windowed<String> key, Double value, String op) {
		String lp = key.key().replace("|", " ")+"_"+op+"="+value.toString()+" "+key.window().end()+"000000";
		return lp;
	}
	
	public static void main(String[] args) throws Exception {
    	ArgumentParser parser = argParser();
        Namespace res = parser.parseArgs(args);
        String config_filename = res.getString("config");
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        YamlDispatcherConfig config = mapper.readValue( new File(config_filename), YamlDispatcherConfig.class);
        
        String input_topic = config.getInput_topic();
        String output_avg_topic = config.getOutput_avg_topic();
        String output_sum_topic = config.getOutput_sum_topic();
        String output_min_topic = config.getOutput_min_topic();
        String output_max_topic = config.getOutput_max_topic();
        //String output_other_topic = config.getOutput_other_topic();
        String log4jfilename = config.getGeneral().get("log4jfilename");
        Map<String,String> kafka_config = config.getkafka_config();
        Map<String,Map<String,String>[]> aggregators = config.getSelection();
        
        PropertyConfigurator.configure(log4jfilename);
    	Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-dispatcher");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "streams-dispatcher-client");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka_config.get("bootstrap_servers"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, Integer.parseInt(kafka_config.getOrDefault("numTheads", "1")));
        
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Double> doubleSerde = Serdes.Double();
        
        Map<String,SimplePair> aggr_conf = new HashMap<String,SimplePair>();
        String meas = null;
        String func = null;
        String tags2rem = null;
        
        // {measurement: (func,tags2remove)}
        for(Map.Entry<String, Map<String,String>[]> entry: aggregators.entrySet()) {
        	for(Map<String,String> item: entry.getValue()) {
        		meas = item.get("measurement");
        		func = entry.getKey();
        		tags2rem = item.getOrDefault("tag_to_remove", "").replaceAll(" ", "");
        		aggr_conf.put(meas, new SimplePair(func,tags2rem));
        	}
        }
        
        for(Map.Entry<String, SimplePair> item: aggr_conf.entrySet()) {
        	System.out.println(item.getKey() + " = " + item.getValue());
        }
        
        final StreamsBuilder builder = new StreamsBuilder();
        try {
	        KStream<String, String> source = builder.stream(input_topic);
	        KStream<String,LineProtocol> lp_data = source.mapValues( 
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
	        
	        @SuppressWarnings("unchecked")
			KStream<String,Triplet<String, Double, String>> branches[] = triplets_data.branch(
	        		(key,value) -> value.getValue2().equals("avg"),
	        		(key,value) -> value.getValue2().equals("sum"),
	        		(key,value) -> value.getValue2().equals("min"),
	        		(key,value) -> value.getValue2().equals("max")
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
	        //branches[4].to(output_other_topic);
        } catch (Exception e) {
        	e.printStackTrace();
        }
             		      
        final Topology topology = builder.build();
 
        logger.info(topology.describe().toString());
        
        final KafkaStreams streams = new KafkaStreams(topology, props);
        
        final CountDownLatch latch = new CountDownLatch(1);
 
        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("aggregator-shutdown-hook") {
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