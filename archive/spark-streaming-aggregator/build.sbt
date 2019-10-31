name := "spark-aggregator"

organization := "ch.cern.alice.o2.spark.streaming"
version := "1.0"
scalaVersion := "2.11.7"


libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.2.1"
libraryDependencies += "org.apache.spark" % "spark-streaming-flume_2.11" % "2.2.1" 
libraryDependencies += "org.apache.flume" % "flume-ng-core" % "1.8.0"
libraryDependencies += "org.apache.flume" % "flume-ng-sdk" % "1.8.0"
libraryDependencies += "org.apache.flume" % "flume-ng-configuration" % "1.8.0"
libraryDependencies += "io.circe" %% "circe-yaml" % "0.8.0"
libraryDependencies += "io.circe" %% "circe-generic-extras" % "0.9.3" 
