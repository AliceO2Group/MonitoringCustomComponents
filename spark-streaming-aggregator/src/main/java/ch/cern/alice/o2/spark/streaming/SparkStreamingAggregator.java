package ch.cern.alice.o2.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function  ;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;
import scala.Tuple2;

import java.nio.charset.*;

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
 *  Usage: SparkStreamingAggregator <receive_host> <receive_port> <db_host> <db_port>
 *    <receive_host> is the host the Flume receiver will be started on - a receiver
 *           creates a server and listens for flume events.
 *    <receive_port> is the port the Flume receiver will listen on.
 *    <db_host> is the host of the InfluxDB instance where to sent UPD packets in line protocol format
 *    <db_port> is the port of the InfluxDB instance where to sent UPD Packets.
 *
 *  To run this example:
 *    `./bin/spark-submit --class ch.cern.alice.o2.spark.streaming.SparkStreamingAggregator \
 *                       --master local[*] /local/spark/jars/spark-streaming-aggregator-1.0-SNAPSHOT.jar \
 *                       <receive_host> <receive_port> <db_host> <db_port>`
 * 
 *  @author Gioacchino Vino
 */
  
  /**
   * 
   * @author Gioacchino Vino
   * This class checks if the field of a line protocol format event is a double type
   *
   *  @param String 
   *  @return Boolean
   */
    class isDouble implements Function<String,Boolean>{
      public Boolean call( String s){
        try {
          Float.parseFloat(s.split(" ")[1].split("=")[1].replace("i",""));
          return true;
        } catch(NumberFormatException e) {
          return false;
        }
      }
    }
    
  /**
   * 
   * @author Gioacchino Vino
   * This class extracts an unique metric name and the double value from a line protocol format event
   * 
   * @param String
   * @return ( String unique_metric_name , (Float value, Float counter)) 
   *
   */
  class extractData implements PairFunction<String,String,Tuple2<Float,Float>>{
    public Tuple2<String,Tuple2<Float,Float>> call (String s){
      return new Tuple2<String,Tuple2<Float,Float>>(
          new String(s.split(" ")[0]+" "+s.split(" ")[1].split("=")[0]),
          new Tuple2<Float,Float>(Float.parseFloat(s.split(" ")[1].split("=")[1].replace("i","")),1.0f));
    }
  }
  
  /**
   * 
   * @author Gioacchino Vino
   * This class imprements the accumulation of values and counters
   * 
   * @param ( String unique_metric_name , (Float value, Float counter))
   * @return ( String unique_metric_name , (Float value, Float counter))
   * 
   */
  class accTuple2Floats implements Function2<Tuple2<Float,Float>,Tuple2<Float,Float>,Tuple2<Float,Float>>{ 
    public Tuple2<Float,Float> call (Tuple2<Float,Float> t1, Tuple2<Float,Float> t2){
      return new Tuple2<Float,Float>(t1._1 + t2._1, t1._2 + t2._2);
    }
  }
  
  /**
   *   
   * @author Gioacchino Vino
   * This class computes the average of each metric_name
   * 
   * @param ( String unique_metric_name , (Float value, Float counter))
   * @return ( String unique_metric_name , Float avg)
   * 
   */
  class evaluateAverage implements PairFunction<Tuple2<String,Tuple2<Float,Float>>,String,Float> {
    public Tuple2<String,Float> call( Tuple2<String,Tuple2<Float,Float>> t){
      return new Tuple2<String,Float>(t._1, t._2._1 / t._2._2);
    }
  }
  
  /**
   * 
   * @author Gioacchino Vino
   * This class returns the line protocol format event from the metric_name and evaluted average
   * 
   * @param ( String unique_metric_name, Float avg)
   * @return String line_protocol
   *
   */
  class createLineProtocol implements Function<Tuple2<String,Float>,String> {
    public String call( Tuple2<String,Float> t){
      return new String(t._1+"_aggr="+t._2+" "+(System.currentTimeMillis()*1000000));
    }
  }
  
  /**
   * 
   * @author Gioacchino Vino
   * This class extracts the body field from a flume event and converts it in a string
   * 
   * @param Flume event
   * @return String
   *
   */
  class FlumeEventToString implements Function<SparkFlumeEvent, String> {
    public String call(SparkFlumeEvent b){
      return new String( b.event().getBody().array(), Charset.forName("UTF-8"));
    }
  }
  
  public final class SparkStreamingAggregator {
  private SparkStreamingAggregator() {
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 4) {
      System.err.println("Usage: SparkStreamingAggregator <receiver_host> <receiver_port> <dst_host> <dst_port>");
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
    Duration batchInterval = new Duration(30000);
    
    // Configuration of Spark object
    SparkConf sparkConf = new SparkConf().setAppName("O2Aggregation");
    JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, batchInterval);
    JavaReceiverInputDStream<SparkFlumeEvent> flumeStream =
      FlumeUtils.createStream(ssc, host, port);

    // Computes the average from the acquired flume events
    JavaDStream<String> lineprotocol = flumeStream.map( new FlumeEventToString() )
        .filter(new isDouble())
        .mapToPair(new extractData())
        .reduceByKey(new accTuple2Floats())
        .mapToPair(new evaluateAverage())
        .map(new createLineProtocol());

    // Send data to InfluxDB
    lineprotocol.foreachRDD(rdd -> {
      rdd.foreachPartition(partitionOfRecords -> {
        DatagramSocket datagramSocket = null;
        try {
          datagramSocket = new DatagramSocket();
        } catch (SocketException e) {
          System.err.println("Error creating datasocket");
        }
        while (partitionOfRecords.hasNext()) {
          byte [] eventBody = partitionOfRecords.next().getBytes(Charset.forName("UTF-8")); 
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