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
 * Implementation of an HTTP sink. Events are POSTed to an HTTP / HTTPS
 * endpoint. The error handling behaviour is configurable, and can respond
 * differently depending on the response status returned by the endpoint.
 *
 * Rollback of the Flume transaction, and backoff can be specified globally,
 * then overridden for ranges (or individual) status codes.
 */
public class InfluxdbUDPSink extends AbstractSink implements Configurable {

  private static final Logger logger = Logger.getLogger(InfluxdbUDPSink.class);

  /** Default InfluxDB host. */
  private String hostname;
  
  /** Default InfluxDB port. */
  private int port;
  
  /** Default InfluxDB Hostname */
  private static final String DEFAULT_HOSTNAME = new String("localhost");
  
  /** Default InfluxDB port */
  private static final int DEFAULT_PORT = 8086;
  
  /** Endpoint URL to POST events to. */
  private InetAddress address;
  
  /** Counter used to monitor event throughput. */
  private SinkCounter sinkCounter;
  
  private DatagramSocket datagramSocket;
  
  //@Override
  public final void configure(final Context context) {
    hostname = context.getString("hostname", DEFAULT_HOSTNAME);
    logger.info("Read InfluxDB hostname from configuration : " + hostname);
    port = context.getInteger("port", DEFAULT_PORT);
    logger.info("Read InfluxDB port from configuration : " + port);
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

  @Override
  public final void start() {
    logger.info("Starting InfluxDB UDP Sink");
    sinkCounter.start();
  }

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
        //LOG.info("UDP InfluxDB Sink Event: " + EventHelper.dumpEvent(event, 100));
        eventBody = event.getBody();
        if (eventBody != null && eventBody.length > 0) {
          sinkCounter.incrementBatchCompleteCount();
          DatagramPacket packet = new DatagramPacket(
              eventBody, eventBody.length, address, port);
          try {
            datagramSocket.send(packet);
            sinkCounter.incrementEventDrainSuccessCount();
            //LOG.debug("Datagram sent");
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
        logger.debug("No data to send");
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