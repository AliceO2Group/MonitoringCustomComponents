package ch.cern.alice.o2.spark.streaming

//import org.apache.hadoop.hbase.spark.HBaseContext
import java.io.IOException
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
import scala.collection.JavaConverters._
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

// /home/ubuntu/spark/bin/spark-submit --jars /home/ubuntu/spark/jars/spark-streaming-kafka-0-10-assembly_2.11-2.2.0.jar -- --class org.vino.DirectKafkaWordCount --master local[4] /home/ubuntu/projects/eventgen10/target/scala-2.11/spark-kafka-project_2.11-1.0.jar	

case class YAMLGeneralConf(appname: String, window: Int)
case class YAMLInputConf(bindaddress: String, port: Int)
//case class YAMLOutputConf(protocol: String, hostname: String, port: Int)
case class YAMLOutputConf(hostname: String, port: Int)
case class YAMLSingleMetricConf(metricname: String, removetags: List[String])
case class YAMLDefaultMetricConf(function: String, removetags: List[String])
                          
object SparkAggregatorLibrary {
  val defaultKey = "default"
  val nameKey = "name"
  val avgKey = "avg"
  val sumKey = "sum"
  val minKey = "min"
  val maxKey = "max"
  val valueKey = "value_value"
  val aggrFunctKey = "aggr_functs"
  val generalKey = "general"
  val inputKey = "input"
  val outputKey = "output"
  
  def importYamlConfig(fileName: String) : Tuple2[Map[String,String],Map[String,Map[String,List[String]]]] = {
    var FunctionConf = Map[String,Map[String,List[String]]]()
    val strFile = Source.fromFile(fileName).mkString
    //println(strFile)
    var Conf = Map[String,Map[String,List[String]]]()
    val json = yaml.parser.parse(strFile).right.get
    val listSubSection = json.hcursor.keys.get.toList
  
    if(!(listSubSection contains generalKey)){
      println("Error: Miss the 'general' section in the confguration file.")
      println("Es. general:\n  appname: SparkAggregator\n  window: 30")
      println("Exit!\n")
      System.exit(2)
    }
    
    if(!(listSubSection contains inputKey)){
      println("Error: Miss the 'input' section in the confguration file")
      println("Es. input:\n  bindaddress: 0.0.0.0\n  port: 7777\n");
      println("Exit!\n")
      System.exit(3)
    }
    
    if(!(listSubSection contains outputKey)){
      println("Error: Miss the 'output' section in the confguration file")
      println("Es. output:\n  protocol: udp\n  hostname: aido2mon1.cern.ch\n  port: 9998")
      println("Exit!\n")
      System.exit(4)
    }
    
    val genConf = json.hcursor.get[YAMLGeneralConf](generalKey).toOption.get
    val inConf = json.hcursor.get[YAMLInputConf](inputKey).toOption.get
    val outConf = json.hcursor.get[YAMLOutputConf](outputKey).toOption.get
    val mConf : Map[String,String] = Map( "SparkAppName" -> genConf.appname,
                                          "SparkBindAddress" -> inConf.bindaddress,
                                          "SparkPort" -> inConf.port.toString,
                                          "OutputProtocol" -> outConf.protocol,
                                          "OutputHostname" -> outConf.hostname,
                                          "OutputPort" -> outConf.port.toString,
                                          "window" -> genConf.window.toString)
                                    
    if( listSubSection contains aggrFunctKey ){
      val listFuncts = json.hcursor.downField(aggrFunctKey).keys.get.toList
      for( f <- listFuncts.filter(_ != defaultKey)){
        for( ff <- json.hcursor.downField(aggrFunctKey).get[List[YAMLSingleMetricConf]](f).toOption.get){
          if( FunctionConf contains ff.metricname){
            FunctionConf(ff.metricname) += (f -> ff.removetags.toList) 
          } else {
            FunctionConf(ff.metricname) = Map(f -> ff.removetags.toList)
          }
        }
      }
      if( listFuncts contains defaultKey){
        val defaulConf = json.hcursor.downField(aggrFunctKey).get[YAMLDefaultMetricConf](defaultKey).toOption.get
        FunctionConf(defaultKey) = Map(defaulConf.function -> defaulConf.removetags.toList)
      } 
    } else {
      println("Warning: Miss the 'aggr_functs' section in the confguration file")
    }
  (mConf,FunctionConf)
  }
  
  def getKeyFromHeaders( headers: Map[String, String] ) : String = {
    var sMetricKey = headers(nameKey).replace("|", "") + "|"
    val lTags = headers.keys.toList.filter( _.contains("tag_")).sorted
    for( key <- lTags ){
      val cleanedKey = key.replace(":","")
      val cleanedValue = headers(key).toString.replace(":","").replace(",","")
      sMetricKey = sMetricKey + cleanedKey + ":" + cleanedValue + ","
    }
    sMetricKey.dropRight(1)
  }
  //val aggr_conf = Map[String,Tuple2[String,Array[String]]]("meas1" -> ("avg",Array("host")), "meas2" -> ("sum",Array("host")))
  
  def toPoint( event: SparkFlumeEvent, mConf: Map[String,Map[String,List[String]]]) : Array[Tuple3[String,Double,String]] = {
    val headers = collection.mutable.Map[String, String]() 
    for( (k:CharSequence, v:CharSequence) <- event.event.getHeaders().asScala ){
      headers += ( k.toString() -> v.toString() )
    }
    
    if( ! headers.contains(nameKey))
      throw new Exception("No name field in the event")
    val sMetricName = (headers get "name").get.toString()
    
    if( ! headers.contains(valueKey))
      throw new Exception("No value_value field in the event")
    
    var sFunctRemTags = Set[Tuple2[String,List[String]]]()
    
    if( mConf contains sMetricName  ){
      sFunctRemTags = mConf(sMetricName).toSet
    } else {
      if( mConf contains defaultKey  ){
        sFunctRemTags = mConf(defaultKey).toSet
      } 
    }
    
    var aPoints = Array[Tuple3[String,Double,String]]()
    var tempHeader = headers.clone()
    for( item <- sFunctRemTags){
      tempHeader = headers.clone()
      for( sTagToRemove <- item._2 if( tempHeader.contains(sTagToRemove)) ){
        if( tempHeader.contains(sTagToRemove))
          tempHeader -= sTagToRemove
      }
      val function = item._1
      val sMetricKey = getKeyFromHeaders(tempHeader)
      val sMetricValue = headers.get(valueKey).get.toString 
      try{
        val dMetricValue = sMetricValue.toDouble
        aPoints :+= (sMetricKey, dMetricValue, function)
      } catch {
        case e : NumberFormatException => throw new Exception("The value_value: '" + sMetricValue + "' is not a number")
        case _ : Throwable => throw new Exception("Impossible parse the value: '" + sMetricValue + "'")
      }
    }
    aPoints
  }
  
  def toAvroEvent( point: (String,Double), value_name: String ) : Event = {
    val splittedPointKey = point._1.split('|')
    var headers = Map[String,String](valueKey -> (splittedPointKey(0)+"_aggr"), ("value_"+value_name) -> point._2.toString, "type_value" -> "double")
    if( splittedPointKey.length > 1 ){
      for (item <- splittedPointKey(1).split(',')){
        val temp1 = item.split(':')
        headers += (temp1(0) -> temp1(1))
      }
    }
    val body : Array[Byte] = Array()
    val event : Event = EventBuilder.withBody(body, headers )
    event
  }
  //Greeting("Hey", Person("Chris"), 3).asJson
  def toJSON( point: (String,Double), timestamp: Long, value_name: String ) : String = {
    val splittedPointKey = point._1.split('|')
    var innerJSON = "name:" + splittedPointKey(0)+"_aggr" + ",value_"+value_name+":" + point._2.toString + ",type_value:double"
    innerJSON = innerJSON + ",timestamp: " + timestamp.toString
    if( splittedPointKey.length > 1 ){
      for (item <- splittedPointKey(1).split(',')){
        val temp1 = item.split(':')
        innerJSON = innerJSON + "," + temp1(0) + ":" + temp1(1)
      }
    }
    val JSON = "{headers: {" + innerJSON + "}}"
    JSON
    //val jsonEvents = avg.map( p => "{\"headers\":{\"name\":\"" + p._1 + "_aggr\"" + ",\"value_value\":\"" + p._2.toString + "\"}}" )
  }
  
}



