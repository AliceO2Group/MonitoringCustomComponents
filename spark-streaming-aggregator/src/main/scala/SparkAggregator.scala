package ch.cern.alice.o2.spark.streaming

import ch.cern.alice.o2.spark.streaming.SparkAggregatorLibrary._

import java.io.IOException
import java.io.File
import java.lang.System
import java.net.DatagramPacket
import java.net.DatagramSocket
import java.net.InetAddress
import java.net.SocketException
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Properties;

import scala.collection.mutable.Map
import scala.collection.JavaConversions._
import scala.io.Source

import io.circe._
import io.circe.generic.auto._
import io.circe.yaml
import cats.syntax.either._

import org.apache.flume.api.RpcClient
import org.apache.flume.api.RpcClientFactory
import org.apache.flume.event.EventBuilder
import org.apache.flume.Event
import org.apache.flume.EventDeliveryException;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.flume._

object SparkAggregator {
  
  val OutputPortKey = "OutputPort"
  val OutputHostnameKey = "OutputHostname"
  val OutputProtocoleKey = "OutputProtocol"

  def main(args: Array[String]) {
    // Import Configuration
    if (args.length != 1) {
        println("You MUST pass ONLY the YAML configuration file path as parameter")
        System.exit(1)
    }
    
    val fileName = args(0)
    if( (new File(fileName)).exists() == false){
      println("File does not exist!")
      System.exit(1)
    }
    
    // Read the configuration parameter from the YAML Reader object
    val (mGeneralConfs,mFunctionConf) = importYamlConfig(fileName)
    val SparkBindAddress = mGeneralConfs("SparkBindAddress")
    val SparkAppName = mGeneralConfs("SparkAppName")
    val SparkPort = mGeneralConfs("SparkPort").toInt
    val OutputProtocol = mGeneralConfs("OutputProtocol")
    val OutputHostname = mGeneralConfs("OutputHostname")
    val OutputPort = mGeneralConfs("OutputPort").toInt
    val window = Seconds(mGeneralConfs("window").toInt)
    
    val sparkConf = new SparkConf().setAppName(SparkAppName)
    val ssc = new StreamingContext(sparkConf, window)
    
    // Broadcast variables
    val bAggregationConf = ssc.sparkContext.broadcast(mFunctionConf)
    val bSenderConf = ssc.sparkContext.broadcast(mGeneralConfs)
    
    // Read Flume event stream
    val flumeStream = FlumeUtils.createStream(ssc, SparkBindAddress , SparkPort)
    
    // 'Data' containing the following items: [(metrickey, value_in_double, aggregation_function),...]
    val data = flumeStream.flatMap( event => toPoint(event,bAggregationConf.value) ).cache()
    
    //Compute the aggregations
    val avg_aggr_data = data.filter( p => p._3 == SparkAggregatorLibrary.avgKey )
                            .map( p => (p._1,(p._2,1)))
                            .reduceByKeyAndWindow( (x: Tuple2[Double,Int], y:Tuple2[Double,Int]) => (x._1+y._1,x._2+y._2), window, window)
                            .map( p => (p._1, p._2._1 / p._2._2.toDouble ) )
    val sum_aggr_data = data.filter( p => p._3 == SparkAggregatorLibrary.sumKey )
                            .map( p => (p._1,p._2))
                            .reduceByKeyAndWindow( (x:Double, y:Double) => (x+y), window, window)
                            
    val max_aggr_data = data.filter( p => p._3 == SparkAggregatorLibrary.maxKey )
                            .map( p => (p._1,p._2))
                            .reduceByKeyAndWindow( (x:Double, y:Double) => ( if( x > y) x else y), window, window)
                            
    val min_aggr_data = data.filter( p => p._3 == SparkAggregatorLibrary.minKey )
                            .map( p => (p._1,p._2))
                            .reduceByKeyAndWindow( (x:Double, y:Double) => ( if( x < y) x else y), window, window)

    val longTimestampNs: Long = System.currentTimeMillis * 1000000

    //Create JSONs and merge all aggregation streams
    val avgJSON = avg_aggr_data.map( toJSON(_, longTimestampNs, SparkAggregatorLibrary.avgKey) )
    val sumJSON = sum_aggr_data.map( toJSON(_, longTimestampNs, SparkAggregatorLibrary.sumKey) )
    val maxJSON = max_aggr_data.map( toJSON(_, longTimestampNs, SparkAggregatorLibrary.maxKey) )
    val minJSON = min_aggr_data.map( toJSON(_, longTimestampNs, SparkAggregatorLibrary.minKey) )
    val jsonEvents = avgJSON.union(sumJSON).union(maxJSON).union(minJSON)
 
    // send to the Flume UDP Source
    jsonEvents.foreachRDD { rdd => 
      rdd.foreachPartition { partitionOfRecords =>
        val datagramSocket = new DatagramSocket();
        val hostname = bSenderConf.value(OutputHostnameKey)
        val port = bSenderConf.value(OutputPortKey).toInt
        val address : InetAddress = InetAddress.getByName(hostname)
        for( event <- partitionOfRecords){
          datagramSocket.send( new DatagramPacket(event.getBytes , event.length(), address , port) )
        }
      }
    }
    
    
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}



