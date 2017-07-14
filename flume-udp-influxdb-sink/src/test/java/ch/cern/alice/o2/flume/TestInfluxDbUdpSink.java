package ch.cern.alice.o2.flume;

import org.apache.flume.Channel;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
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

import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Charsets;

/**
 * Code to test Apache flume InfluxDB UDP Sink
 * @author Gioacchino Vino
 */


public class TestInfluxDbUdpSink {
  private static final String hostname = "127.0.0.1";
  private static final Integer port = 25639;
  
  private InfluxDbUdpSink sink;
  private Channel channel;
  
  /**
   * Configure the channel and the sink for the test
   */
  public void setUp() {
    sink = new InfluxDbUdpSink();
    channel = new MemoryChannel();
    Context sink_context = new Context();
    Context channel_context = new Context();
    sink_context.put("hostname", hostname);
    sink_context.put("port", String.valueOf(port));
    sink.setChannel(channel);
    Configurables.configure(sink, sink_context);
    Configurables.configure(channel, channel_context);
  }
 
  /**
   * Check whether the event sent using the sink is egual to that received 
   * @throws SocketException
   * @throws EventDeliveryException
   */
  @Test
  public void testReceivePacket() throws SocketException, EventDeliveryException {
    setUp();
    DatagramSocket serverSocket = new DatagramSocket(port);
    byte[] receiveData = new byte[1024];
    DatagramPacket receivePacket = new DatagramPacket(receiveData,
                       receiveData.length);
    
    Event event = new SimpleEvent();
    Map<String,String> headers = new HashMap<String, String>();
    headers.put("name", "testMetric");
    headers.put("value", "120");
    headers.put("hostname", "testhostname.cern.ch");
    headers.put("timestamp", "1500036047850741446");
    event.setHeaders(headers);
    sink.start();
    Transaction transaction = channel.getTransaction();
    transaction.begin();
    for (int i = 0; i < 10; i++) {
      channel.put(event);
    }
    transaction.commit();
    transaction.close();

    for (int i = 0; i < 10; i++) {
      Sink.Status status = sink.process();
      Assert.assertEquals(Sink.Status.READY, status);
      try {
        serverSocket.receive(receivePacket);
        String receivedFromSink = new String(receivePacket.getData(), 0,
                           receivePacket.getLength() );
        String expected = new String("testMetric,hostname=testhostname.cern.ch value=120 1500036047850741446");
        Assert.assertEquals(receivedFromSink, expected);
      } catch (IOException e) {
        System.out.println(e);
      }
      
    }
    Assert.assertEquals(Sink.Status.BACKOFF, sink.process());
    
    sink.stop();
    serverSocket.close();
  }
}
