# Spark Streaming Pass Through

## Build
1. Clone repository
 ~~~
 git clone https://github.com/AliceO2Group/MonitoringCustomComponents.git 
 cd MonitoringCustomComponents/spark-streaming-pass-through
 ~~~
2. Compile
 ~~~
 mvn clean -e install -DskipTests
 ~~~

## Run
1. Move created `.jar` file from `target/` to Spark's `jars/` directory
2. Configure a Flume agent having a Avro Sink 

*Example:*
 ~~~
agent.sources = my_source
agent.sources.my_source.channels = channel_mem

agent.channels = channel_mem
agent.channels.channel_mem.type = memory

agent.sinks = avro_sink
agent.sinks.avro_sink.type = avro
agent.sinks.avro_sink.channel = channel_mem
agent.sinks.avro_sink.hostname = <receiver_host>
agent.sinks.avro_sink.port = <receiver_port>
 ~~~

3. Submit the Spark Streaming job
```
$SPARK_HOME/bin/spark-submit --class ch.cern.alice.o2.spark.streaming.SparkStreamingAggregator \
  --master local[*] $SPARK_HOME/jars/spark-streaming-pass-through-1.0-SNAPSHOT.jar \ 
  <receiver_bind> <receiver_port> <db_host> <db_port> <batch_interval>
```

Description of run parameters
| Property name  | Default | Description |
| -------------- | ------- | ----------- |
| receiver_host  |         | |
| receiver_bind  |         | Must be set to 0.0.0.0 |
| receiver_port  |         | |
| db_host  |         | Host where to send UDP packet |
| db_port  |         | Port where to send UDP packet |
| batch_interval  |         | Batch interval in milliseconds of Spark Streaming job. > 20ms |
