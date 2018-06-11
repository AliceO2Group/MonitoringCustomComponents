# Spark Streaming aggregator

The aggregator receives the metric values from a [Flume Avro Sink](https://flume.apache.org/FlumeUserGuide.html#avro-sink), computes the aggregation using a given function and send the results in event format to a [Flume UDP Source](https://github.com/AliceO2Group/MonitoringCustomComponents/tree/master/flume-udp-source).

## Dependencies
- Spark > 2.2.0
- Scala >= 2.10
- Flume > 1.7.0

### Package Dependencies
- [spark-streaming-flume-assembly_2.11-2.2.0](https://search.maven.org/remotecontent?filepath=org/apache/spark/spark-streaming-flume-assembly_2.11/2.2.0/spark-streaming-flume-assembly_2.11-2.2.0.jar)
- [spark-streaming-flume_2.11-2.2.0](https://search.maven.org/remotecontent?filepath=org/apache/spark/spark-streaming-flume_2.11/2.2.0/spark-streaming-flume_2.11-2.2.0.jar)
- [snakeyaml-1.20](central.maven.org/maven2/org/yaml/snakeyaml/1.20/snakeyaml-1.20.jar)
- [circe-yaml-0.8.0](central.maven.org/maven2/io/circe/circe-yaml_2.10/0.8.0/circe-yaml_2.10-0.8.0.jar)
- [circe-core-0.9.3](central.maven.org/maven2/io/circe/circe-core_2.10/0.9.3/circe-core_2.10-0.9.3.jar)
- [circe-numbers-0.9.3](central.maven.org/maven2/io/circe/circe-numbers_2.10/0.9.3/circe-numbers_2.10-0.9.3.jar)
- [circe-generic-0.9.3](central.maven.org/maven2/io/circe/circe-generic_2.10/0.9.3/circe-generic_2.10-0.9.3.jar)
- [cats-core-1.0.1](central.maven.org/maven2/org/typelevel/cats-core_2.10/1.0.1/cats-core_2.10-1.0.1.jar)
- [cats-kernel-1.0.1](central.maven.org/maven2/org/typelevel/cats-kernel_2.10/1.0.1/cats-kernel_2.10-1.0.1.jar)

## Build
1. Clone repository
```
 git clone https://github.com/AliceO2Group/MonitoringCustomComponents.git && cd spark-streaming-aggregator  
```
2. Compile
```
 sbt package
```

## Run
1. Move created `.jar` file from `target/` to Spark's `jars/` directory
2. Download the jars listed in **Package Dependencies**
3. Edit the YAML configuration file
4. Enable Flume's Avro Sink to pass values to Spark

*Example:*
```
agent.sources = my_source my_source1
agent.sources.my_source.channels = channel_mem

agent.channels = channel_mem
agent.channels.channel_mem.type = memory

agent.sinks = avro_sink
agent.sinks.avro_sink.type = avro
agent.sinks.avro_sink.channel = channel_mem
agent.sinks.avro_sink.hostname = <receiver_host>
agent.sinks.avro_sink.port = <receiver_port>

```

5. Insert in the Spark's `jars/` directory the dependencies :
- `spark-streaming-flume-assembly_2.11-2.2.0.jar`
- `spark-streaming-flume_2.11-2.2.0.jar`
- `snakeyaml-1.20.jar`
- `circe-yaml_2.10-0.8.0.jar`
- `circe-core_2.10-0.9.3.jar`
- `circe-numbers_2.10-0.9.3.jar`
- `circe-generic_2.10-0.9.3.jar`
- `cats-core_2.10-1.0.1.jar`
- `cats-kernel_2.10-1.0.1.jar`

6. Submit the Spark Streaming job
 ~~~
 $SPARK_HOME/bin/spark-submit --class ch.cern.alice.o2.spark.streaming.SparkStreamingAggregator \
  --master local[*] $SPARK_HOME/jars/spark-streaming-aggregator-1.0-SNAPSHOT.jar <config.yaml>
 ~~~
 
 
## YAML Coonfiguration
The YAML configuration file format is used to pass parameters to the aggregation job.

*Example:*
```
general:
  appname: SparkAggregator
  window: 30

input:
  bindaddress: 0.0.0.0
  port: 7777

output:
  protocol: udp
  hostname: aido2mon1.cern.ch
  port: 9998 #udp

aggr_functs:
  default:
    function: avg
    removetags: [ tag_host ]
  avg:
    - metricname: metricname1
      removetags: [ tag_host, tag_volumes ]
  sum:
    - metricname: metricname1
      removetags: [ tag_host, tag_path ]
  max:
    - metricname: metricname3
      removetags: [ tag_host ]
  min:
    - metricname: metricname3
      removetags: [ ] #in this case no tags will be removed and the aggregation will preserve all tags.
```

YAML configuration file rules:
- The `general`, `input` and `output` section are mandatory
- The `aggr_functs` is optional even if unuseless
- The sub keyword `default`, `ævg`, `sum`, `max` and `min` are optional but if inserted a metricname/removetags list MUST be provided
- All key words ( `general`, `appname`, `input`, ...) MUST be lowercase
- The `window` and `port` parameters MUST be integer
- In the aggregation function section the `default` keyword is used to aggregate all metrics without the need to list all of them


Tab. 1
| **Allowed functions** | Description | Returned value |
| ----------| -----------------| ------- |
| *avg* | Compute the average | Double |
| *sum* | Compute the sum     | Double |
| *max* | Extract the maximum | Double | 
| *min* | Extract the minimum | Double |


Tab. 2
|  Section  |  First Keyword   |  Second Keyword(s)   | Mandatory | Description |
| ----------| -----------------| ------- | ----------- |
| *general* | -          | -  | Yes    | Define the start of 'general' configuration section |
| *general* | *addname*  | -  |  Yes    | Name to assign to the aggregation job. Es: "SparkAggregationJob" |
| *general* | *window*   | -  |  Yes    | Define the start of 'general' configuration section |
| *input*   | -          | -  |  Yes    | Define the start of 'input' configuration section and inner Avro Source related parameters must be inserted |
| *input*   | *bind*     | -  |  Yes    | Bind address. Es "0.0.0.0" |
| *input*   | *port*     | -  |  Yes    | Port where listen to |
| *output*  | -          | -  |  Yes    | Define the start of 'output' configuration section and UDP transmission related parameters must be inserted |
| *output*  | *hostname* | -  |  Yes    | Hostname where send data |
| *output*  | *port*     | -  |  Yes    | Port where send data |
| *aggr_functs* | -       | -  |  No     | Define the start of 'aggregation functionst' configuration section |
| *aggr_functs*  | *allowed_function*  | -  |  No    | One of allowed aggregation functions. Requires a list of {*metricname* and *removetags* }|
| *aggr_functs*  | *allowed_function*  | *metricname*  |  Yes (if one of *allowed_function* is present) | Metric name to aggregate |
| *aggr_functs*  | *allowed_function*  | *removetags*  |  Yes (if one of *allowed_function* is present) | Tags to remove during the aggregation phase. If you want a global value for all host, just type [tag_host]|
| *aggr_functs*  | *defaut*  | -  |  No    | Define aggregation function and which tags remove of not specified metric names |
| *aggr_functs*  | *defaut*  | *function*  |  Yes (if *default* is present) | Function to use in the aggregation. Only the function listed in the Tab1 are allowed |
| *aggr_functs*  | *defaut*  | *removetags*  |  Yes (if *default* is present) | Tags to remove during the aggregation phase. If you want a global value for all host, just type [tag_host]|


