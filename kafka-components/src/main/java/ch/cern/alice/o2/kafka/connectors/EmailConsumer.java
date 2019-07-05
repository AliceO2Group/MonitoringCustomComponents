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
import java.util.Set;

import java.util.Collections;
import java.util.HashMap;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.log4j.PropertyConfigurator;
import org.json.JSONException;
import org.json.JSONObject;
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

import ch.cern.alice.o2.kafka.utils.YamlEmailConsumer;

import org.apache.commons.mail.Email;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.SimpleEmail;

public class EmailConsumer {
	private static Logger logger = LoggerFactory.getLogger(EmailConsumer.class); 
	
	/* Email private vars */
	private static String email_hostname = "";
	private static int email_port = 0;
	private static String email_username = "";
	private static String email_password = "";
	private static String email_from = "";
	
	/* Stats private vars */
	private static int stats_endpoint_port = 0;
	private static long receivedRecords = 0;
	private static long sentRecords = 0;
	private static long stats_period_ms = 0;
	private static long startMs = 0;
	private static String stats_type;
	private static InetAddress stats_address = null;
	private static DatagramSocket datagramSocket;
	
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
	private static final String DEFAULT_ENABLE_AUTO_COMMIT_CONFIG = "false";
	private static final String DEFAULT_GROUP_ID_CONFIG = "email-consumer";
	private static final int POLLING_PERIOD_MS = 50;
	
	public EmailConsumer(String hostname, int port, String username, String password, String from) {
		email_hostname = hostname;
		email_port = port;
		email_username = username;
		email_password = password;
		email_from = from;
	}
	
	public void sendEmail(String json_data) {
		try {
			Map<String,String> email_data = parseJSON(json_data);
    		final Email email = new SimpleEmail();
    		email.setHostName(email_hostname);
    		email.setSmtpPort(email_port);
    		email.setFrom(email_from);
    		email.setAuthentication(email_username, email_password);
    		email.setSubject(email_data.get("subject"));
    		email.setMsg(email_data.get("body"));
    		for( String address : email_data.get("to_addresses").split(",")) {
    			email.addTo(address);
    		}
    		email.send();
    		sentRecords++;
    		logger.info("Mail sent to: " + json_data);
    	} catch (final EmailException e) {
    		logger.error(e.getMessage(), e);
    	} catch (final JSONException  e) {
    		logger.error(e.getMessage(), e);
    	} catch (final Exception  e) {
    		logger.error(e.getMessage(), e);
    	}
	}
	
	private static Map<String,String> parseJSON( String input_json) throws Exception {
		Map<String,String> data = new HashMap<String,String>();
		JSONObject input_json_obj = new JSONObject(input_json);
        Set<String> fields = input_json_obj.keySet();
        if( fields.contains("to_addresses") && fields.contains("subject") && fields.contains("body")) {
        	data.put("to_addresses", input_json_obj.getString("to_addresses") );
        	data.put("body", input_json_obj.getString("body") );
        	data.put("subject", input_json_obj.getString("subject") );
        	return data;
        } else {
        	throw new Exception("'body', 'subject' or 'to_addresses' field(s) are not in the JSON: " + input_json);
        }
	}
	
	public static void main(String[] args) throws Exception {
        String stats_endpoint_hostname = null;
        startMs = System.currentTimeMillis(); 
		
        /* Parse command line argumets */
        ArgumentParser parser = argParser();
        Namespace res = parser.parseArgs(args);
        String config_filename = res.getString("config");
        
        /* Parse yaml configuration file */
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        YamlEmailConsumer config = mapper.readValue( new File(config_filename), YamlEmailConsumer.class);
        String log4jfilename = config.getGeneral().get("log4jfilename");
        PropertyConfigurator.configure(log4jfilename);
        Map<String,String> kafka_consumer_config = config.getKafka_consumer_config();
        Map<String,String> email_config = config.getEmail_config();
        Map<String,String> stats_config = config.getStats_config();
        String topicName = kafka_consumer_config.get("topic");
        String email_hostname = email_config.get("hostname");
        int email_port = Integer.parseInt(email_config.get("port"));
        String email_username = email_config.get("username");
        String email_password = email_config.get("password");
        String email_from = email_config.get("from");
        
        /* Emailer Configuration */
        EmailConsumer emailer = new EmailConsumer(email_hostname, email_port, email_username, email_password, email_from); 
        
        /* Stats Configuration */
        boolean stats_enabled = Boolean.valueOf(stats_config.getOrDefault("enabled", DEFAULT_STATS_ENABLED));
        stats_type = DEFAULT_STATS_TYPE;
        stats_endpoint_hostname = stats_config.getOrDefault("hostname", DEFAULT_HOSTNAME);
        stats_endpoint_port = Integer.parseInt(stats_config.getOrDefault("port", DEFAULT_PORT));
        stats_period_ms = Integer.parseInt(stats_config.getOrDefault("period_ms", DEFAULT_STATS_PERIOD));
        logger.info("Stats Enabled?: "+ stats_enabled);
        
        if( stats_enabled ) {
	        logger.info("Stats Endpoint Hostname: "+stats_endpoint_hostname);
	        logger.info("Stats Endpoint Port: "+stats_endpoint_port);
	        logger.info("Stats Period: "+stats_period_ms+"ms");
	        try {
	        	stats_address = InetAddress.getByName(stats_endpoint_hostname);
	        } catch (IOException e) {
	          logger.error("Error opening creation address using hostname: "+stats_endpoint_hostname, e);
	        }
	        try {
		        datagramSocket = new DatagramSocket();
		      } catch (SocketException e) {
		        logger.error("Error while creating UDP socket", e);
		    }
        }
        
        /* Configure Kafka consumer */
    	Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, DEFAULT_GROUP_ID_CONFIG);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, DEFAULT_ENABLE_AUTO_COMMIT_CONFIG);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka_consumer_config.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafka_consumer_config.getOrDefault(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, DEFAULT_AUTO_OFFSET_RESET));
    	
        KafkaConsumer<byte[],byte[]> consumer = new KafkaConsumer<byte[],byte[]>(props);
        consumer.subscribe(Collections.singletonList(topicName));
        
        while (true) {
			ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(POLLING_PERIOD_MS);
			receivedRecords += consumerRecords.count();
			if( stats_enabled ) stats();
            for (ConsumerRecord<byte[], byte[]> record : consumerRecords) {
            	emailer.sendEmail(new String(record.value()));
            }
		}
	}
	
	private static void stats() throws IOException {
		long nowMs = System.currentTimeMillis();
    	if ( nowMs - startMs > stats_period_ms) {
    		startMs = nowMs;
    		String hostname = InetAddress.getLocalHost().getHostName();
			if(stats_type.equals(STATS_TYPE_INFLUXDB)) {
	    		String data2send = "kafka_consumer,endpoint_type=Emailer,hostname="+hostname;
				data2send += " receivedRecords="+receivedRecords+"i,sentRecords="+sentRecords+"i "+nowMs+"000000";
				DatagramPacket packet = new DatagramPacket(data2send.getBytes(), data2send.length(), stats_address, stats_endpoint_port);
		        datagramSocket.send(packet);
			}
    	}
	}
    
	private static ArgumentParser argParser() {
        @SuppressWarnings("deprecation")
		ArgumentParser parser = ArgumentParsers
            .newArgumentParser("email-consumer")
            .defaultHelp(true)
            .description("This tool is used to send Emails.");
        parser.addArgument("--config")
    	    .action(store())
            .required(true)
            .type(String.class)
            .dest("config")
            .help("config file");
        return parser;
    }
}	