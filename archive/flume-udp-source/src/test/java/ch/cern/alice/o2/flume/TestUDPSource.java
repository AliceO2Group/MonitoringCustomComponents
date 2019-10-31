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

import java.util.ArrayList;
import java.util.List;

import java.net.SocketException;
import java.net.UnknownHostException;
import java.net.InetAddress;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.MulticastSocket;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;

public class TestUDPSource {
  private UDPSource source;
  private Channel   channel;

  private static final String UNICAST_HOST   = "127.0.0.1";
  private static final int    UNICAST_PORT   = 14453;

  private static final String testMessage    = "{\"body\": \"test\",\"headers\": {\"test\": \"testValue\"}}";

  @Before
  public void setUp() {
    source  = new UDPSource(); 
    channel = new MemoryChannel();

    Configurables.configure(channel, new Context());

    List<Channel> channels = new ArrayList<Channel>();
    channels.add(channel);

    ChannelSelector rcs = new ReplicatingChannelSelector();
    rcs.setChannels(channels);

    source.setChannelProcessor(new ChannelProcessor(rcs));
  }

  @Test
  public void testUnicast() throws Exception {
    Event          event       = null;
    DatagramSocket socket      = null;
    InetAddress    address     = null;

    // configure the UDPSource
    Context context = new Context();
    context.put("host", UNICAST_HOST);
    context.put("port", String.valueOf(UNICAST_PORT));
    source.configure(context);

    // initialize sockets and packet to send.
    socket  = new DatagramSocket(); 
    address = InetAddress.getByName(UNICAST_HOST); 
    String testMessageWithNewline = testMessage + "\n";
    byte[] sendData = testMessageWithNewline.getBytes();
    DatagramPacket packet = new DatagramPacket(sendData, sendData.length, address, UNICAST_PORT);
    
    // start the UDPSource
    source.start();

    // send the packet.
    socket.send(packet);
    
    // read the packet from the channel the UDPSource is part of.
    Transaction txn = channel.getTransaction();
    try {
      txn.begin();
      event = channel.take();
      txn.commit();
    } finally {
      txn.close();
    }
    
    // stop the source
    source.stop();
      
    // assert that the packet was read correctly.
    Assert.assertNotNull(event);
  }
}
