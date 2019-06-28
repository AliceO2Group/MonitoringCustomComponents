package ch.cern.alice.o2.kafka.connectors;

import ch.cern.alice.o2.kafka.utils.YamlMattermostConfig;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.URL;
import javax.net.ssl.HttpsURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;
import java.util.Collections;
import java.util.Set;
import java.util.HashSet;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.log4j.PropertyConfigurator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.json.*;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;

import static net.sourceforge.argparse4j.impl.Arguments.store;

public class MattermostConsumer {
	private static Logger logger = LoggerFactory.getLogger(MattermostConsumer.class);
	private static Set<String> allowed_fields = new HashSet<String>();
	private static String mattermost_url;
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
        YamlMattermostConfig config = mapper.readValue( new File(config_filename), YamlMattermostConfig.class);
        Map<String,String> kafka_consumer_config = config.getKafka_consumer_config();
        Map<String,String> mattermost_config = config.getmattermost_config();
        Map<String,String> stats_config = config.getStats_config();
        mattermost_url = mattermost_config.get("url");
        String topicName = kafka_consumer_config.get("topic");
        boolean stats_enabled = Boolean.valueOf(stats_config.getOrDefault("enabled", DEFAULT_STATS_ENABLED));
        stats_type = stats_config.getOrDefault("type", DEFAULT_STATS_TYPE);
        stats_endpoint_hostname = stats_config.getOrDefault("hostname", DEFAULT_HOSTNAME);
        stats_endpoint_port = Integer.parseInt(stats_config.getOrDefault("port", DEFAULT_PORT)); 
        stats_period_ms = Integer.parseInt(stats_config.getOrDefault("period_ms", DEFAULT_STATS_PERIOD));

        /* Log configuration */
        String log4jfilename = config.getGeneral().get("log4jfilename");
		PropertyConfigurator.configure(log4jfilename);
		
		/* Stats Configuration */
        if( stats_enabled ) {
	        logger.info("Stats Type: "+ stats_type);
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
		
        logger.info("InputTopic: " + topicName);
        
        /* Kafka Configuration */
    	Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "mattermost-conns");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, DEFAULT_ENABLE_AUTO_COMMIT_CONFIG);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka_consumer_config.get("bootstrap.servers"));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafka_consumer_config.getOrDefault("auto.offset.reset", DEFAULT_AUTO_OFFSET_RESET));
      
		KafkaConsumer<byte[],byte[]> consumer = new KafkaConsumer<byte[],byte[]>(props);
        consumer.subscribe(Collections.singletonList(topicName));
        
        while (true) {
			ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(POLLING_PERIOD_MS);
			receivedRecords += consumerRecords.count();
			if( stats_enabled ) stats();
            for (ConsumerRecord<byte[], byte[]> record : consumerRecords) {
            	send(new String(record.value()));
            }
        }
	}

	private static void send(String json_data) throws Exception {
		URL obj = new URL(mattermost_url);
		HttpsURLConnection con = (HttpsURLConnection) obj.openConnection();
		
		if(isJSONValid(json_data)) {
			String data_to_send = getJSON(json_data);
			byte[] byte_data = data_to_send.getBytes( StandardCharsets.UTF_8 );
			int    DataLength = byte_data.length;
			con.setRequestMethod("POST");
			con.setRequestProperty("User-Agent", "Mozilla/5.0");
			con.setRequestProperty("Accept-Language", "en-US,en;q=0.5");
			con.setRequestProperty("Content-Type", "application/json");
			con.setRequestProperty("charset", "utf-8");
			con.setRequestProperty("Content-Length", Integer.toString( DataLength ));
			con.setDoOutput(true);

			try( DataOutputStream wr = new DataOutputStream( con.getOutputStream())) {
				   wr.write( byte_data );
				   sentRecords++;
			}
			int responseCode = con.getResponseCode();
			if( responseCode != 200) {
				logger.error("ResponseCode: "+responseCode);
			}
		} else {
			logger.warn("Error not valid JSON format: "+json_data);
		}
	}
	
	private static void stats() throws IOException {
		long nowMs = System.currentTimeMillis();
    	if ( nowMs - startMs > stats_period_ms) {
    		startMs = nowMs;
    		String hostname = InetAddress.getLocalHost().getHostName();
			if(stats_type.equals(STATS_TYPE_INFLUXDB)) {
	    		String data2send = "kafka_consumer,endpoint_type=Mattermost,url="+mattermost_url+",hostname="+hostname;
				data2send += " receivedRecords="+receivedRecords+"i,sentRecords="+sentRecords+"i "+nowMs+"000000";
				DatagramPacket packet = new DatagramPacket(data2send.getBytes(), data2send.length(), stats_address, stats_endpoint_port);
		        datagramSocket.send(packet);
			}
    	}
	}
	
	public static boolean isJSONValid(String test) {
		JSONObject input_json_obj = new JSONObject();
	    try {
	    	input_json_obj = new JSONObject(test);
	    } catch (JSONException ex) {
	        try {
	            new JSONArray(test);
	        } catch (JSONException ex1) {
	            return false;
	        }
	    }
	    if( input_json_obj.keySet().contains("description") )
	    	return true;
	    else
	    	return false;
	}
	
	public static String getJSON(String input_json) {
		String output_json = "";
	    try {
	        JSONObject input_json_obj = new JSONObject(input_json);
	        Set<String> fields = input_json_obj.keySet();
	        //fields.retainAll(allowed_fields);
	        JSONObject output_json_obj = new JSONObject();
	        String msg = input_json_obj.getString("description");
	        
	        if( fields.contains("client_url")) {
	        	msg += "\n" + input_json_obj.getString("client_url");
	        }
	        if( fields.contains("details")) {
	        	msg += "\n" + input_json_obj.getString("details");
	        } 
	        
	        output_json_obj.put("text", msg);
	        output_json = output_json_obj.toString();
	    } catch (JSONException ex) {
	    	logger.warn("Error during JSON extration from: "+input_json+"Exception: "+ex);
	    	return new String();
	    }
	    return output_json;
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
