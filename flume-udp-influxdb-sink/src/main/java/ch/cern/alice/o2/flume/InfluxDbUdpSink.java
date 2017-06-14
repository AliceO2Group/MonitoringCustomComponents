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
    logger.info("Read InfluxDB UDP hostname from configuration : " + hostname);
    port = context.getInteger("port", DEFAULT_PORT);
    logger.info("Read InfluxDB UDP port from configuration : " + port);
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
      logger.error("Error creating datasocket", e);
    }
  }
  
  /**
   * Initialization step: start the sinkcounter used for 
   * statistical and monitoring purpose
   */
  @Override
  public final void start() {
    logger.info("Starting InfluxDB UDP Sink");
    sinkCounter.start();
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
    Status status = Status.READY;
    byte[] eventBody = null;
    Event event = null;
    Channel ch = getChannel();
    Transaction txn = ch.getTransaction();
    
    try {
      txn.begin();
      event = ch.take();
      if (event != null) {
        eventBody = event.getBody();
        if (eventBody != null && eventBody.length > 0) {
          sinkCounter.incrementBatchCompleteCount();
          DatagramPacket packet = new DatagramPacket(
              eventBody, eventBody.length, address, port);
          try {
            datagramSocket.send(packet);
            sinkCounter.incrementEventDrainSuccessCount();
          } catch (IOException e) {
            logger.error("Error send packet. ", e);
            status = Status.BACKOFF;
          }
        } else {
          logger.debug("eventBody == null");
        }
      } else {
        sinkCounter.incrementBatchEmptyCount();
        status = Status.BACKOFF;
        logger.debug("No events extracted from channel");
      }
      txn.commit();
    } catch (Exception e) {
      logger.error(e);
      txn.rollback();
      throw new EventDeliveryException("Failed to log event: " + event, e);
    } finally {
      txn.close();
    }
    return status;
  }
}