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
package ch.cern.alice.o2.kafka.connectors;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.Map;
import java.util.Properties;
import java.util.Arrays;
import java.util.Collections;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import static net.sourceforge.argparse4j.impl.Arguments.store;

import java.net.SocketException;
import java.io.File;

import ch.cern.alice.o2.kafka.utils.YamlInfluxdbUdpConsumer;

public class InfluxdbUdpConsumer {
	private static Logger logger = LoggerFactory.getLogger(InfluxdbUdpConsumer.class); 
	private static String data_endpoint_port_str = "";
	private static int [] data_endpoint_ports = null;
	private static int data_endpoint_ports_size = 0;
	private static int data_endpoint_ports_index = 0;
	private static int stats_endpoint_port = 0;
	private static long receivedRecords = 0;
	private static long sentRecords = 0;
	private static long stats_period_ms = 0;
	private static long startMs = 0;
	private static String stats_type;
	private static String data_endpoint_hostname =  null;
	private static InetAddress data_address = null;
	private static InetAddress stats_address = null;
	private static DatagramSocket datagramSocket;
	private static String topicName = "";
	
	/* Stats parameters */
	private static final String STATS_TYPE_INFLUXDB = "influxdb";
	private static final String DEFAULT_STATS_TYPE = STATS_TYPE_INFLUXDB;
	private static final String DEFAULT_STATS_PERIOD = "10000";
	private static final String DEFAULT_STATS_ENABLED = "false";
	
	/* UDP sender parameters */
	private static final String DEFAULT_HOSTNAME = "localhost";
	private static final String DEFAULT_PORT = "8089";
	
	/* Kafka consumer parameters */
	private static final String DEFAULT_AUTO_OFFSET_RESET = "latest";
	private static final String DEFAULT_FETCH_MIN_BYTES = "1";
	private static final String DEFAULT_RECEIVE_BUFFER_BYTES = "262144";
	private static final String DEFAULT_MAX_POLL_RECORDS = "1000000";
	private static final String DEFAULT_ENABLE_AUTO_COMMIT_CONFIG = "false";
	private static final String DEFAULT_GROUP_ID_CONFIG = "influxdb-udp-consumer";
	private static final int POLLING_PERIOD_MS = 50;
	
	public static void main(String[] args) throws Exception {
        String stats_endpoint_hostname = null;
        startMs = System.currentTimeMillis(); 
		
        /* Parse command line argumets */
        ArgumentParser parser = argParser();
        Namespace res = parser.parseArgs(args);
        String config_filename = res.getString("config");
        
        /* Parse yaml configuration file */
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        YamlInfluxdbUdpConsumer config = mapper.readValue( new File(config_filename), YamlInfluxdbUdpConsumer.class);
        String log4jfilename = config.getGeneral().get("log4jfilename");
        PropertyConfigurator.configure(log4jfilename);
        Map<String,String> kafka_consumer_config = config.getKafka_consumer_config();
        Map<String,String> sender_config = config.getSender_config();
        Map<String,String> stats_config = config.getStats_config();
        topicName = kafka_consumer_config.get("topic");
		data_endpoint_hostname = sender_config.getOrDefault("hostname",DEFAULT_HOSTNAME);
		data_endpoint_port_str = sender_config.getOrDefault("port",DEFAULT_PORT);
        String[] data_endpoint_ports_str = data_endpoint_port_str.split(",");
        data_endpoint_ports = new int[data_endpoint_ports_str.length];
        for( int i=0; i < data_endpoint_ports_str.length; i++) {
        	data_endpoint_ports[i] = Integer.parseInt(data_endpoint_ports_str[i]);
        }
        data_endpoint_ports_size = data_endpoint_ports_str.length;
        
        boolean stats_enabled = Boolean.valueOf(stats_config.getOrDefault("enabled", DEFAULT_STATS_ENABLED));
        stats_type = DEFAULT_STATS_TYPE;
        stats_endpoint_hostname = stats_config.getOrDefault("hostname", DEFAULT_HOSTNAME);
        stats_endpoint_port = Integer.parseInt(stats_config.getOrDefault("port", DEFAULT_PORT));
        stats_period_ms = Integer.parseInt(stats_config.getOrDefault("period_ms", DEFAULT_STATS_PERIOD));
        
		logger.info("Data Endpoint Hostname: "+ data_endpoint_hostname);
		logger.info("Data Endpoint Port(s): " + data_endpoint_port_str);
        logger.info("Stats Enabled?: "+ stats_enabled);
        
        /* UDP Configuration */
        try {
        	data_address = InetAddress.getByName(data_endpoint_hostname);
        } catch (IOException e) {
          logger.error("Error opening creation address using hostname: "+data_endpoint_hostname, e);
        }
        
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
        
        /* Configure Kafka consumer */
        Properties props = new Properties();
    	props.put(ConsumerConfig.GROUP_ID_CONFIG, kafka_consumer_config.getOrDefault(ConsumerConfig.GROUP_ID_CONFIG,DEFAULT_GROUP_ID_CONFIG));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, DEFAULT_ENABLE_AUTO_COMMIT_CONFIG);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka_consumer_config.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafka_consumer_config.getOrDefault(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, DEFAULT_AUTO_OFFSET_RESET));
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, kafka_consumer_config.getOrDefault(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, DEFAULT_FETCH_MIN_BYTES));
        props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, kafka_consumer_config.getOrDefault(ConsumerConfig.RECEIVE_BUFFER_CONFIG, DEFAULT_RECEIVE_BUFFER_BYTES));
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, kafka_consumer_config.getOrDefault(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, DEFAULT_MAX_POLL_RECORDS));
    	
        KafkaConsumer<byte[],byte[]> consumer = new KafkaConsumer<byte[],byte[]>(props);
        consumer.subscribe(Collections.singletonList(topicName));
     
        while (true) {
        	try {
                ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(POLLING_PERIOD_MS);
                receivedRecords += consumerRecords.count();
                consumerRecords.forEach( record -> sendUdpData(record.value()));
        		consumer.commitAsync();
        		if( stats_enabled ) stats();
            } catch (RetriableCommitFailedException e) {
        		logger.error("Kafka Consumer Committ Exception: ",e);
        		consumer.close();
        		break;
        	} catch (Exception e) {
    			logger.error("Kafka Consumer Error: ",e);
    			consumer.close();
        		break;
    		}
        }
	}
	
	private static void sendUdpData(byte[] data2send) {
		try {
			int data_port = data_endpoint_ports[(data_endpoint_ports_index++)%data_endpoint_ports_size];
			DatagramPacket packet = new DatagramPacket(data2send, data2send.length, data_address, data_port );
	        datagramSocket.send(packet);
	        sentRecords++;
		} catch (Exception e) {
			logger.error("Error: ",e);
		}
	}
	
	
	
	private static void stats() throws IOException {
		long nowMs = System.currentTimeMillis();
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
            .newArgumentParser("influxdb-udp-consumerr")
            .defaultHelp(true)
            .description("This tool is used to send UDP packets to InfluxDB.");

        parser.addArgument("--config")
    	    .action(store())
            .required(true)
            .type(String.class)
            .dest("config")
            .help("config file");

        return parser;
    }
}	
