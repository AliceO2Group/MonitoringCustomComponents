package ch.cern.alice.o2.flume;

import org.apache.flume.Channel;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.Sink;
import org.apache.flume.Transaction;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.*;

import com.google.common.base.Charsets;

public class TestHTTPInfluxdbSink {
  private static final Logger logger = LoggerFactory
      .getLogger(TestHTTPInfluxdbSink.class);
  
  private static final String  hostname = "127.0.0.1";
  private static final Integer port = 12346;
  private static final String  database = "mydb";
  private static final Integer batchSize = 1000;
  
  private InfluxDbHttpSink sink;
  private Channel channel;
  
  public void setUp() {
    sink = new InfluxDbHttpSink();
    channel = new MemoryChannel();
    Context sink_context = new Context();
    Context channel_context = new Context();
    sink_context.put("hostname", hostname);
    sink_context.put("port", String.valueOf(port));
    sink_context.put("database", database);
    sink_context.put("batchSize", String.valueOf(batchSize));
    sink.setChannel(channel);
    Configurables.configure(sink, sink_context);
    Configurables.configure(channel, channel_context);
  }
  
  @Test
  public void testReceivePacket() throws EventDeliveryException, IOException {
    setUp();
    ServerSocket serverSocket = new ServerSocket(port);
    String line = null;
    String temp = null;
    String eventBody = new String(
        "meas,tag1=key1 value=100i 1234567890");
    Event event = EventBuilder.withBody( eventBody, Charsets.UTF_8);
    sink.start();
    Transaction transaction = channel.getTransaction();
    transaction.begin();
    for (int i = 0; i < 1; i++) {
      channel.put(event);
    }
    transaction.commit();
    transaction.close();

    Sink.Status status = sink.process();
    String body = null;
    Assert.assertEquals(Sink.Status.BACKOFF, status);
    try {
      Socket clientSocket = serverSocket.accept();
      BufferedReader in = new BufferedReader (
          new InputStreamReader(clientSocket.getInputStream()));
      while ((line = in.readLine()) != null) {
        temp = line;
    }
      Assert.assertEquals(eventBody, temp);
    } catch (IOException e) {
      System.out.println(e);
    }
      
    sink.stop();
    serverSocket.close();
  }
}

