package ch.cern.alice.o2.flume;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.http.HTTPSourceHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;
import java.util.HashMap;

/**
 * CollectdHandler for HTTPSource that accepts an array of events.
 *
 * This handler throws exception if the deserialization fails because of bad
 * format or any other reason.
 *
 * Each HTTP request containing a Collectd data in JSON format is encoded in 
 * one o multiple Flume event(s). All the Collectd JSON fields are encoded in 
 * the event header whereas the body is left empty.
 * The used headers are: 
 * - "name": it contains the metric name;
 * - "tag_host": it contains the host tag;
 * - "tag_instance": it contains the plugin_instance tag;
 * - "tag_type": it contains the collectd type tag;
 * - "tag_type_instance": it contains the collectd type_instance tag;
 * - "timestamp":it contains the metric timestamp;
 * - "value_value":it contains the metric value;  
 * - "type_value": it contains the type value. Could be "long" or "double".
 */

public class JSONCollectdHTTPHandler implements HTTPSourceHandler {

  private static final Logger LOG = LoggerFactory.getLogger(JSONCollectdHTTPHandler.class);
  private final JsonParser parser;
  private final String TYPE_COUNTER = new String("counter");
  private final String TYPE_ABSOLUTE = new String("absolute");
  private final String TYPE_DERIVE = new String("derive");
  private final String TYPE_GAUGE = new String("gauge");
  private final String VALUETYPE_LONG = new String("long");
  private final String VALUETYPE_FLOAT = new String("double");
  private final String COLLECTD_KEY_HOST = new String("host");
  private final String COLLECTD_KEY_TIMESTAMP = new String("time");
  private final String COLLECTD_KEY_PLUGIN = new String("plugin");
  private final String COLLECTD_KEY_PLUGIN_INSTANCE = new String("plugin_instance");
  private final String COLLECTD_KEY_TYPE = new String("type");
  private final String COLLECTD_KEY_TYPE_INSTANCE = new String("type_instance");
  private final String COLLECTD_KEY_DSNAMES = new String("dsnames");
  private final String COLLECTD_KEY_DSTYPES = new String("dstypes");
  private final String COLLECTD_KEY_VALUES = new String("values");
  private final String EVENT_KEY_HOST = new String("tag_host");
  private final String EVENT_KEY_TIMESTAMP = new String("timestamp");
  private final String EVENT_KEY_NAME = new String("name");
  private final String EVENT_KEY_PLUGIN_INSTANCE = new String("tag_instance");
  private final String EVENT_KEY_TYPE = new String("tag_type");
  private final String EVENT_KEY_TYPE_INSTANCE = new String("tag_type_instance");
  private final String EVENT_KEY_VALUE = new String("value_value");
  private final String EVENT_KEY_VALUETYPE = new String("type_value");
  private final String EVENT_KEY_SEPARATOR = new String("_");
  public JSONCollectdHTTPHandler() {
    parser = new JsonParser();
  }

  public List<Event> getEvents(HttpServletRequest request) throws Exception {
    BufferedReader reader = request.getReader();
    String line = reader.readLine();
    ArrayList<Event> eventList = new ArrayList<Event>();
    try {
      JsonElement jsonTree = parser.parse(line);
      if(jsonTree.isJsonArray()){
    	  JsonArray jsonArray = jsonTree.getAsJsonArray();
    	  for( JsonElement metric: jsonArray){
    		  try {
	    		  String sTimestamp = null;
	    		  Double dTimestamp = (double) 0.0;
	    		  String sHost = null;
	    		  String sPlugin = null;
	    		  String sPluginInstance = null;
	    		  String sType = null;
	    		  String sTypeInstance = null;
	    		  String sValue = null;
	    		  String sLocalType = null;
	    		  
	    		  JsonObject jobject = metric.getAsJsonObject();
	    		  dTimestamp = jobject.get(COLLECTD_KEY_TIMESTAMP).getAsDouble() * 1000000000;
	    		  sTimestamp = String.valueOf( dTimestamp.longValue() );
	    		  sHost = jobject.get(COLLECTD_KEY_HOST).getAsString();
	    		  sPlugin = jobject.get(COLLECTD_KEY_PLUGIN).getAsString();
	    		  sPluginInstance = jobject.get(COLLECTD_KEY_PLUGIN_INSTANCE).getAsString();
	    		  sType = jobject.get(COLLECTD_KEY_TYPE).getAsString();
	    		  sTypeInstance = jobject.get(COLLECTD_KEY_TYPE_INSTANCE).getAsString();
	    		  
	    		  if(sHost.isEmpty() || sHost == null) 
	    			  throw new Exception("Host invalid: " + sHost);
	    		  
	    		  if(sPlugin.isEmpty() || sPlugin == null)
	    			  throw new Exception("Plugin invalid: " + sPlugin);
	    		  
	    		  JsonArray typesArray = jobject.get(COLLECTD_KEY_DSTYPES).getAsJsonArray();
	    		  int iTypesArraySize = typesArray.size();
	    		  String [] asTypes = new String[iTypesArraySize];
	    		  for( int iTypeIndex = 0; iTypeIndex < iTypesArraySize; iTypeIndex++){
	    			  asTypes[iTypeIndex] = typesArray.get(iTypeIndex).getAsString();
	    		  }
	    		  JsonArray namesArray = jobject.get(COLLECTD_KEY_DSNAMES).getAsJsonArray();
	    		  int iNamesArraySize = namesArray.size();
	    		  String [] asNames = new String[iNamesArraySize];
	    		  for( int iNameIndex = 0; iNameIndex < iNamesArraySize; iNameIndex++){
	    			  asNames[iNameIndex] = namesArray.get(iNameIndex).getAsString();
	    		  }
	    		  
	    		  JsonArray valuesArray = jobject.get(COLLECTD_KEY_VALUES).getAsJsonArray();
	    		  int iValuesArraySize = valuesArray.size();
	    		  if( iValuesArraySize != iNamesArraySize || iValuesArraySize != iTypesArraySize )
	    			  throw new Exception("value-type-name array size mismatch");
	    		  for( int iValuesIndex = 0; iValuesIndex < iValuesArraySize; iValuesIndex++){
	    			  String siType = asTypes[iValuesIndex];
	    			  String siName = asNames[iValuesIndex];
	    			  if( siType.equals(TYPE_COUNTER) || siType.equals(TYPE_ABSOLUTE) || siType.equals(TYPE_DERIVE)){
	    			    sValue = String.valueOf(valuesArray.get(iValuesIndex).getAsLong());
	    			    sLocalType = VALUETYPE_LONG;
	    			  } else {
	    				  if(siType.equals(TYPE_GAUGE) ){
	    					  sValue = String.valueOf(valuesArray.get(iValuesIndex).getAsDouble());
	    					  sLocalType = VALUETYPE_FLOAT;
	    				  } else {
	    					  throw new Exception("Type not match: "+siType);
	    				  }
	    			  }
	    			  if( !sValue.isEmpty() || sValue != null) {
	    				  Map<String, String> eventHeader = new HashMap<String, String>();
	    				  if( !sPluginInstance.isEmpty() && sPluginInstance != null) 
	    	    			  eventHeader.put(EVENT_KEY_PLUGIN_INSTANCE, sPluginInstance);
	    	    		  if( !sType.isEmpty() && sType != null) 
	    	    			  eventHeader.put(EVENT_KEY_TYPE, sType);
	    	    		  if( !sTypeInstance.isEmpty() && sTypeInstance != null) 
	    	    			  eventHeader.put(EVENT_KEY_TYPE_INSTANCE, sTypeInstance);
	    	    		  if( !sTimestamp.isEmpty() && sTimestamp != null) 
	    	    			  eventHeader.put(EVENT_KEY_TIMESTAMP, sTimestamp);
	    	    		  eventHeader.put(EVENT_KEY_HOST, sHost);
						  eventHeader.put(EVENT_KEY_VALUE, sValue);
						  eventHeader.put(EVENT_KEY_NAME, sPlugin+EVENT_KEY_SEPARATOR+siName);
						  eventHeader.put(EVENT_KEY_VALUETYPE, sLocalType);
						  Event event = new SimpleEvent();
			    		  event.setHeaders(eventHeader);
			    		  LOG.debug("Event: " + event);
						  if(event != null) 
			    			  eventList.add(event);
					  }
	    		  }
	    	  } catch (Exception ex) {
    			  LOG.warn("Exception: " + ex);
    			  continue;
    		  }
    	  }
      }
    } catch (JsonSyntaxException ex) {
      throw new Exception("Request has invalid JSON Syntax.", ex);
    }
    int iEventListSize = eventList.size(); 
    LOG.info("Parsed "+iEventListSize+" elements from the HTTP packet");
    return eventList;
  }
  
  public void configure(Context context) {
  }
}