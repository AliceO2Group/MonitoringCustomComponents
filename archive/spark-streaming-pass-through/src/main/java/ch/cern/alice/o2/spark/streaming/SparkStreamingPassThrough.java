package ch.cern.alice.o2.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.io.IOException;

/**
 *  Computes the average of numeric value received from Flume. 
 *  
 *  This should be used in conjunction with an AvroSink in Flume. It will start
 *  an Avro server on at the request host:port address and listen for requests.
 *  Your Flume AvroSink should be pointed to this address.
 *  
 *  This uses a Flume event with a line protocol (influxdb) format in the body.
 *  
 *  This sent the average value to a InfluxDB instance using the UDP protocol and line 
 *  protocol format. 
 *
 *  Usage: SparkStreamingAggregator <receiver_host> <receiver_port> <db_host> <db_port>
 *    <receiver_host> is the host the Flume receiver will be started on - a receiver
 *           creates a server and listens for flume events.
 *    <receiver_port> is the port the Flume receiver will listen on.
 *    <db_host> is the host of the InfluxDB instance where to sent UPD packets in line protocol format
 *    <db_port> is the port of the InfluxDB instance where to sent UPD Packets.
 *
 *  To run this example:
 *    `./bin/spark-submit --class ch.cern.alice.o2.spark.streaming.SparkStreamingAggregator \
 *                       --master local[*] /local/spark/jars/spark-streaming-aggregator-1.0-SNAPSHOT.jar \
 *                       <receiver_host> <receiver_port> <db_host> <db_port>`
 * 
 *  @author Gioacchino Vino
 */
  
  /**
   * 
   * @author Gioacchino Vino
   * This class extracts the body field from a flume event and converts it in a string
   * 
   * @param Flume event
   * @return String
   *
   */
  class FlumeEventToBytes implements Function<SparkFlumeEvent, byte[]> {
    public byte[]  call(SparkFlumeEvent b){
      return b.event().getBody().array();
    }
  }
  
  public final class SparkStreamingPassThrough {
  private SparkStreamingPassThrough() {
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 5) {
      System.err.println("Usage: SparkStreamingAggregator <receiver_host> <receiver_port> <dst_host> <dst_port> <batch_interval>");
      System.exit(1);
    }
    
    // host the Flume receiver
    String host = args[0];
    
    // port the Flume receiver
    int port = Integer.parseInt(args[1]);
    
    // host of the InfluxDB instance
    String db_host = new String(args[2]);
    InetAddress db_address = InetAddress.getByName(db_host);
    
    // port of the InfluxDB instance
    int db_port = Integer.parseInt(args[3]);
    
    // Batch Interval used
    int iBatchInterval = Integer.parseInt(args[4]);
    Duration batchInterval = new Duration(iBatchInterval);
    
    // Configuration of Spark object
    SparkConf sparkConf = new SparkConf().setAppName("O2Spark-pass-through");
    JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, batchInterval);
    JavaReceiverInputDStream<SparkFlumeEvent> flumeStream =
      FlumeUtils.createStream(ssc, host, port);

    // Computes the average from the acquired flume events
    //JavaDStream<byte[]> bytesToWrite = flumeStream.map( new FlumeEventToBytes() );
    JavaDStream<byte[]> bytesToWrite = flumeStream.map( b -> b.event().getBody().array() );
    
    // Send data to InfluxDB
    bytesToWrite.foreachRDD(rdd -> {
      rdd.foreachPartition(partitionOfRecords -> {
        DatagramSocket datagramSocket = null;
        try {
          datagramSocket = new DatagramSocket();
        } catch (SocketException e) {
          System.err.println("Error creating datasocket");
        }
        while (partitionOfRecords.hasNext()) {
          byte [] eventBody = partitionOfRecords.next(); 
          DatagramPacket packet = new DatagramPacket(
              eventBody, eventBody.length, db_address , db_port );
          try {
            datagramSocket.send(packet);
          } catch (IOException e) {
            System.err.println("Error send packet. ");
          }
        }
      });
    });

    // Start
    ssc.start();
    ssc.awaitTermination();
  }
}
