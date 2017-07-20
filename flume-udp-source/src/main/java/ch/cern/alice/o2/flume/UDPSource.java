/*
 * Based on:  
 * - SyslogUDPSource (Apache Flume Syslog UDP Source)
 * - https://github.com/ottomata/flume-ng/tree/udp-source
 * Apache License 2.0. See:
 * - https://github.com/apache/flume/blob/trunk/NOTICE
 * - https://github.com/apache/flume/blob/trunk/LICENSE
 */

package ch.cern.alice.o2.flume;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;

import java.net.UnknownHostException;
import java.net.SocketException;

import java.io.ByteArrayOutputStream;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.Configurables;
import org.apache.flume.source.AbstractSource;

import org.jboss.netty.bootstrap.ConnectionlessBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.socket.DatagramChannel;
import org.jboss.netty.channel.socket.DatagramChannelFactory;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.oio.OioDatagramChannelFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonParser;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Set;
import java.util.Map.Entry;
import java.nio.ByteBuffer;

/**
 * Apache flume UDP/JSON Source
 * It allows to parse JSON-encoded metrics received via UDP channel into Flume events
 * @author Adam Wegrzynek
 */
public class UDPSource extends AbstractSource implements EventDrivenSource, Configurable {

  /**
   * UDP socket hostname
   * (Can be set via Flume agent configuration file or UDPSourceConfigurationConstants class)
   */
  private String host;
 
  /** 
   * UDP socket port
   * (Can be set via Flume agent configuration file or UDPSourceConfigurationConstants class)
   */
  private int port;

  /** 
   * Delimiter that splits events
   * (Can be set via Flume agent configuration file or UDPSourceConfigurationConstants class)
   */
  private char delimiter;

  /** 
   * Maximum event size
   * (Can be set via Flume agent configuration file or UDPSourceConfigurationConstants class)
   */
  private int maxSize;

  /**
   * Output mode:
   *  - event - create Flume event based on JSON formatted message
   *  - pass  - pass to Flume event body filed
   * (Can be set via Flume agent configuration file or UDPSourceConfigurationConstants class)
   */
  private String mode;

  /**
   * Netty UDP channel
   */
  private DatagramChannel nettyChannel;

  /**
   * Flume logger
   */
  private static final Logger logger = LoggerFactory.getLogger(UDPSource.class);

  /**
   * Counts collected/dropped metrics
   */
  private CounterGroup counterGroup = new CounterGroup();

  @Override
  public void start() {
    // setup Netty server
    DatagramChannelFactory datagramChannelFactory =
      new OioDatagramChannelFactory(Executors.newCachedThreadPool());

    ConnectionlessBootstrap serverBootstrap =
      new ConnectionlessBootstrap(datagramChannelFactory);

    final UDPChannelHandler handler = new UDPChannelHandler(maxSize, delimiter);
    
    serverBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
      @Override
      public ChannelPipeline getPipeline() {
        return Channels.pipeline(handler);
      }
    });

    nettyChannel = (DatagramChannel)serverBootstrap.bind(new InetSocketAddress(host, port));

    super.start();
    logger.info("UDP/JSON Source started, mode: " + mode);
  }

  @Override
  public void stop() {
    logger.info("UDP/JSON Source stopping...");
    if (nettyChannel != null) {
      nettyChannel.close();
      try {
        nettyChannel.getCloseFuture().await(60, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        logger.warn("Netty server stop interrupted", e);
      } finally {
        nettyChannel = null;
      }
    }

    super.stop();
  }

  @Override
  public void configure(Context context) {
    Configurables.ensureRequiredNonNull(context, UDPSourceConfigurationConstants.CONFIG_PORT);

    host = context.getString(UDPSourceConfigurationConstants.CONFIG_HOST,
      UDPSourceConfigurationConstants.DEFAULT_HOST);

    port = context.getInteger(UDPSourceConfigurationConstants.CONFIG_PORT);

    delimiter = context.getString(UDPSourceConfigurationConstants.CONFIG_DELIMITER,
      UDPSourceConfigurationConstants.DEFAULT_DELIMITER).charAt(0);

    maxSize = context.getInteger(UDPSourceConfigurationConstants.CONFIG_MAXSIZE,
      UDPSourceConfigurationConstants.DEFAULT_MAXSIZE);

    mode = context.getString(UDPSourceConfigurationConstants.CONFIG_MODE,
      UDPSourceConfigurationConstants.DEFAULT_MODE);
  }

  /**
   * Channel handler that reads metrics from UDP sockets, decode JSON and creates
   * Flume events
   */
  public class UDPChannelHandler extends SimpleChannelHandler {
    protected int maxSize;
    protected char delimiter;
    protected ByteArrayOutputStream baos;

    public UDPChannelHandler(int maxSize, char delimiter) {
      super();
      this.maxSize = maxSize;
      this.delimiter = delimiter;
      this.baos = new ByteArrayOutputStream(this.maxSize);
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent mEvent) {
      try {
        Event e = extractEvent((ChannelBuffer)mEvent.getMessage());
        if (e == null) {
          return;
        }
        getChannelProcessor().processEvent(e);
        counterGroup.incrementAndGet("events.collected");
      } catch (ChannelException ex) {
        counterGroup.incrementAndGet("events.dropped");
        logger.error("Error writting to channel", ex);
        return;
      }
    }

    /**
     * Extract bytes needed for building Flume event
     * Events are split on delimiter, or by maxSize bytes
     * @param in Netty channel
     * @return SimpleEvent with empty body
     */
    public Event extractEvent(ChannelBuffer in) {
      Event e = null;

      try {
        while (in.readable()) {
          if (baos.size() > this.maxSize) throw new Exception("Event too large");
          byte b = in.readByte();
          // stop reading if end of line
          if (b == delimiter) {
            break;
          } else {
            baos.write(b);
          }
        }
        if (mode.equals("event")) {
          e = buildEvent();
        } else if (mode.equals("pass")) {
          e = passAsBody();
        } else {
          throw new Exception("Wrong output mode");
        }
      } catch (Exception ex) {
        // clear buffer for the next event
        baos.reset();
        logger.error("Event building failed: " + ex.getMessage());
      } finally {
        // no-op
      }
      return e;
    }

    /**
    * Passes recevied message into Flume body
    */
    Event passAsBody() {
      byte[] body;
      body = baos.toByteArray();
      baos.reset();
      return EventBuilder.withBody(body);
    }

    /**
     * Encodes JSON and builds Flume event
     */
    Event buildEvent() {
      // create event witout body
      Event e = new SimpleEvent();
      // parse json
      JsonObject jsonEvent = new JsonParser().parse(baos.toString()).getAsJsonObject();
      JsonObject jsonHeaders = jsonEvent.getAsJsonObject("headers");
      Map<String,String> headers = new HashMap<String, String>();
      Set<Entry<String, JsonElement>> entrySet = jsonHeaders.entrySet();
      // interate over header
      for (Map.Entry<String,JsonElement> entry : entrySet) {
         headers.put(entry.getKey(), jsonHeaders.get(entry.getKey()).toString().replaceAll("^\"|\"$", ""));
      } 
      e.setHeaders(headers);
      // clear buffer
      baos.reset();
      return e;
    }
  }
}
