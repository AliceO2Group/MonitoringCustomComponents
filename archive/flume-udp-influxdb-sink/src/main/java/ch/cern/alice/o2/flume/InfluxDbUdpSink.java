/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package ch.cern.alice.o2.flume;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.log4j.Logger;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.io.IOException;

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

/**
 * Apache flume InfluxDB UDP Sink
 * It allows to send line protocol format events to an instance on 
 * InfluxDB via UDP. 
 * 
 * Two modes could be selected:
 *  - pass: the component excepts to find in the event body the InfluxDB Line Protocol. 
 *          It parse the body as a string and sent it to InfluxDB instance without any check.
 *  - event: the component excepts to find in the event headers data to sent to the InfluxDB instance.
 *           It parse the headers, check them and convert them to the InfluxDB Line Procotol.
 *           The header must have, at least, the name and the value fields. Since the component is able to
 *           sent multiple values in a single packet, in order to discern multiple value fields from others
 *           the "value_" prefix must be used, e.g. "value_usage_idle", "value_bytesread".
 *           The name field represents the metric name and the required field name is "name". 
 *           Optionally could be used the tags and timestamp fields.
 *           "timestamp" field must be represented in nano-second unit.
 *           Tag fields must be have the "tag_" prefix in order to be detected from the component.
 *           E.g. "tag_host", "tag_nic", "tag_datacenter"
 *           All fields not beloning to those won't be sent to the InfluxDB instance.  
 * 
 * @author Gioacchino Vino
 */

public class InfluxDbUdpSink extends AbstractSink implements Configurable {
  
  
  /** Flume logger */
  private static final Logger logger = Logger.getLogger(InfluxDbUdpSink.class);

  /** InfluxDB hostname. */
  private String hostname;
  
  /** Default InfluxDB Hostname */
  private static final String DEFAULT_HOSTNAME = new String("localhost");
  
  /** InfluxDB port. */
  private int port;
  
  /** Default InfluxDB port */
  private static final int DEFAULT_PORT = 8089;

  /** Input data mode: event, pass */
  private boolean passThrough;

  /** Default input data mode */
  private static final String DEFAULT_MODE = new String("pass");
  
  /** Endpoint URL to POST events to. */
  private InetAddress address;
  
  /** Counter used to monitor event throughput. */
  private SinkCounter sinkCounter;
  
  /** Prefix of 'tag' fields in flume event */
  private String tagPrefix = new String("tag_");
  
  /** Prefix of 'field' fields in flume event */
  private String valuePrefix = new String("value_");
  
  /** 'measurement' field in flume event */
  private String measField = new String("name");
  
  /** 'timestamp' field in flume event */
  private String timestampField= new String("timestamp");
  
  /** Prefix of 'field' fields in flume event */
  private int tagPrefixLenght = tagPrefix.length();
  
  /** Prefix of 'field' fields in flume event */
  private int valuePrefixLenght = valuePrefix.length();
  
  /** Prefix of 'field' fields in flume event */
  private int minPrefixLenght = 0;
  
  /** UnixTimestamp of 01/01/2018 in nano-seconds*/
  private long minTimestampNs= 1514764800000000000L;
  
  /** class used to send UDP packet  */
  private DatagramSocket datagramSocket;
  
  /**
   * Import configuration parameter from the configuration file 
   */
  
  //@Override
  public final void configure(final Context context) {
    hostname = context.getString("hostname", DEFAULT_HOSTNAME);
    port = context.getInteger("port", DEFAULT_PORT);
    try {
      address = InetAddress.getByName(hostname);
    } catch (IOException e) {
      logger.error("Error opening creation address. ", e);
    }
    if (this.sinkCounter == null) {
      this.sinkCounter = new SinkCounter(this.getName());
    }

    try {
      datagramSocket = new DatagramSocket();
    } catch (SocketException e) {
      logger.error("Error while creating UDP socket", e);
    }
    String mode = context.getString("mode", DEFAULT_MODE);
    passThrough = true;
    if (mode.equals("event")) {
      passThrough = false;
    } else if (!mode.equals("pass")) {
      logger.error("Wrong input mode, fallback to pass mode");
    }
    
    if( tagPrefixLenght <= valuePrefixLenght ) 
      minPrefixLenght = tagPrefixLenght;
    else 
      minPrefixLenght = valuePrefixLenght;
  }
  
  /**
   * Initialization step: start the sinkcounter used for 
   * statistical and monitoring purpose
   */
  @Override
  public final void start() {
    sinkCounter.start();
    logger.info("InfluxDB/UDP Sink started " + hostname + ":" + port);
  }

  /**
   * Closing step: stop the sinkcounter used for 
   * statistical and monitoring purpose
   */
  @Override
  public final void stop() {
    logger.info("Stopping InfluxDB UDP Sink");
    sinkCounter.stop();
  }
  
  //@Override
  public final Status process() throws EventDeliveryException {
    Status status = null;
    DatagramPacket packet = null;
    Channel ch = getChannel();
    Transaction txn = ch.getTransaction();
    txn.begin();
    try {
      Event event = ch.take();
      if (event == null) {
        sinkCounter.incrementBatchEmptyCount();
        txn.commit();
        return Status.BACKOFF;
      }
      sinkCounter.incrementBatchCompleteCount();
      try {
        if (passThrough) {
          packet = passFromBody(event);
        } else {
          packet = createFromEvent(event);
        }
        datagramSocket.send(packet);
        sinkCounter.incrementEventDrainSuccessCount();
      } catch (EventDeliveryException ex) {
        logger.error(ex);
      }
      txn.commit();
      status = Status.READY;
    } catch (Exception e) {
      logger.error(e);
      txn.rollback();
      status = Status.BACKOFF;
      throw new EventDeliveryException("Failed to log event", e);
    } finally {
      //txn.commit();
      txn.close();
    }
    return status;
  }
  
  
  private final String parseValue( String value){
    String strEncodedValue = new String();
    try{
      Long.valueOf(value);
      strEncodedValue = value + "i";
    } catch (NumberFormatException ex_long) {
      try {
        Double.parseDouble(value);
        strEncodedValue = value;
      } catch (NumberFormatException ex_double) {
        strEncodedValue = "\""+value+"\"";
      }
    }
    return strEncodedValue;
  }
  
  private final DatagramPacket createFromEvent(Event event) throws EventDeliveryException {
    Map<String,String> headers = new HashMap<String, String>();
    List<String> tags = new ArrayList<String>(); 
    List<String> values = new ArrayList<String>();
    Long timestamp = 0L;
    headers = event.getHeaders();
    logger.debug("Event Headers:" + headers.toString());
    if(!headers.containsKey(measField)) {
      throw new EventDeliveryException("Header does not contain name field");
    }
    if(headers.containsKey(timestampField)) {
      try{
        timestamp = Long.parseLong(headers.get(timestampField));
        if( timestamp < minTimestampNs){
          throw new EventDeliveryException("Timestamp must be in nanoseconds");
        }
      } catch (NumberFormatException ex){
        throw new EventDeliveryException("Timestamp field is not long type");  
      }
    }
    for(String strKey : headers.keySet()) {
      if(strKey.length() > minPrefixLenght ){
        if( strKey.substring(0, tagPrefixLenght).contains(tagPrefix)){
          tags.add(strKey);
        } else {
          if( strKey.substring(0, valuePrefixLenght).contains(valuePrefix)){
            values.add(strKey);
          } 
        }
      }
    }
    if(values.isEmpty()) {
      throw new EventDeliveryException("Header does not contain any value fields");
    }
    String influxMessage = headers.get(measField);
    if(!tags.isEmpty()){
      for( String tagKey : tags){
        influxMessage += "," + tagKey.substring(tagPrefixLenght) + "=" + headers.get(tagKey);
      }
    }
    String [] arrayValues = new String[values.size()];
    arrayValues = values.toArray(arrayValues);
    influxMessage += " " + arrayValues[0].substring(valuePrefixLenght) + "=" + parseValue(headers.get(arrayValues[0]));
    for( int ii = 1; ii < values.size(); ii++){
      influxMessage += "," + arrayValues[ii].substring(valuePrefixLenght) + "=" + parseValue(headers.get(arrayValues[ii]));
    }
    if( timestamp != 0L){
      influxMessage += " " + headers.get("timestamp");
    }
    logger.debug("InfluxdbLineProtocol: " + influxMessage);
    return new DatagramPacket(influxMessage.getBytes(), influxMessage.length(), address, port);
  }

  private final DatagramPacket passFromBody(Event event) throws EventDeliveryException {
    byte[] eventBody = event.getBody();
    if (eventBody == null && eventBody.length < 1) {
      throw new EventDeliveryException("Fume Event body empty");
    }
    return new DatagramPacket(eventBody, eventBody.length, address, port);
  }
}
