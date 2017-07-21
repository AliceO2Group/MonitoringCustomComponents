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

/**
 * Apache flume InfluxDB UDP Sink
 * It allows to send line protocol format events to an instance on 
 * InfluxDB via UDP. 
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
      if (passThrough) {
        packet = passFromBody(event);
      } else {
        packet = createFromEvent(event);
      }
      datagramSocket.send(packet);
      sinkCounter.incrementEventDrainSuccessCount();
      txn.commit();
      status = Status.READY;
    } catch (Exception e) {
      logger.error(e);
      txn.rollback();
      status = Status.BACKOFF;
      throw new EventDeliveryException("Failed to log event", e);
    } finally {
      txn.close();
    }
    return status;
  }

  private final DatagramPacket createFromEvent(Event event) throws EventDeliveryException {
    Map<String,String> headers = new HashMap<String, String>();
    headers = event.getHeaders();
    if (!headers.containsKey("name") && !headers.containsKey("value")) {
      throw new EventDeliveryException("Header does not contain name and value fields");
    }
    String influxMessage = "%s,hostname=%s value=%s %s";
    String boundParams = String.format(influxMessage,
      headers.get("name"), headers.get("hostname"), headers.get("value"), headers.get("timestamp")
    );
    return new DatagramPacket(boundParams.getBytes(), boundParams.length(), address, port);
  }

  private final DatagramPacket passFromBody(Event event) throws EventDeliveryException {
    byte[] eventBody = event.getBody();
    if (eventBody == null && eventBody.length < 1) {
      throw new EventDeliveryException("Fume Event body empty");
    }
    sinkCounter.incrementBatchCompleteCount();
    return new DatagramPacket(eventBody, eventBody.length, address, port);
  }
}
