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

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.io.IOException;

import com.google.common.base.Charsets;

public class TestInfluxdbUDPSink {
  private static final Logger logger = LoggerFactory
      .getLogger(TestInfluxdbUDPSink.class);
  
  private static final String hostname = "127.0.0.1";
  private static final Integer port = 25639;
  
  private InfluxdbUDPSink sink;
  private Channel channel;
  
  public void setUp() {
    sink = new InfluxdbUDPSink();
    channel = new MemoryChannel();
    Context sink_context = new Context();
    Context channel_context = new Context();
    sink_context.put("hostname", hostname);
    sink_context.put("port", String.valueOf(port));
    sink.setChannel(channel);
    Configurables.configure(sink, sink_context);
    Configurables.configure(channel, channel_context);
  }
  
  @Test
  public void testReceivePacket() throws SocketException, EventDeliveryException {
    setUp();
    DatagramSocket serverSocket = new DatagramSocket(port);
    byte[] receiveData = new byte[1024];
    DatagramPacket receivePacket = new DatagramPacket(receiveData,
                       receiveData.length);
    
    String eventBody = new String(
        "meas,tag1=key1 value=100i 1234567890");
    Event event = EventBuilder.withBody( eventBody, Charsets.UTF_8);
    
    sink.start();
    Transaction transaction = channel.getTransaction();
    transaction.begin();
    for (int i = 0; i < 10; i++) {
      channel.put(event);
    }
    transaction.commit();
    transaction.close();

    for (int i = 0; i < 10; i++) {
      //logger.info("ITERATION: " + i);
      Sink.Status status = sink.process();
      Assert.assertEquals(Sink.Status.READY, status);
      try {
        serverSocket.receive(receivePacket);
        String body = new String( receivePacket.getData(), 0,
                           receivePacket.getLength() );
        //System.out.println(body + "\n");
        Assert.assertEquals(eventBody, body);
      } catch (IOException e) {
        System.out.println(e);
      }
      
    }
    Assert.assertEquals(Sink.Status.BACKOFF, sink.process());
    
    sink.stop();
    serverSocket.close();
  }
}
