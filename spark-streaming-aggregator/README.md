# Spark Streaming aggregator

This component computes aggregations of values coming from [Flume Avro Sink](https://flume.apache.org/FlumeUserGuide.html#avro-sink) using one of the allowed aggregation functions and sends the results back to a [Flume UDP Source](https://github.com/AliceO2Group/MonitoringCustomComponents/tree/master/flume-udp-source) using its format.

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
2. Download the jars listed in **Package Dependencies** and move them to the Spark's `jars/` directory
3. Edit the YAML configuration file
4. Configure a Flume Avro Sink to transmit values to Spark (the input values MUST have the format described below)

*Example:*
```
agent.sources = my_source1 my_source2
agent.sources.my_source1.channels = channel_mem1
agent.sources.my_source2.channels = channel_mem2

agent.channels = channel_mem1 channel_mem2
agent.channels.channel_mem1.type = memory
agent.channels.channel_mem2.type = memory

agent.sinks = avro_sink 
agent.sinks.avro_sink.type = avro
agent.sinks.avro_sink.channel = channel_mem
agent.sinks.avro_sink.hostname = <rcv_hostname>
agent.sinks.avro_sink.port = <rcv_port>

agent.sources.my_source2.type = ch.cern.alice.o2.flume.UDPSource
agent.sources.my_source2.port = <dst_port>

...
```

5. Submit the Spark Streaming job
```
$SPARK_HOME/bin/spark-submit --class ch.cern.alice.o2.spark.streaming.SparkStreamingAggregator \
  --master local[*] $SPARK_HOME/jars/spark-streaming-aggregator-1.0-SNAPSHOT.jar <config.yaml>
```
 
 
## YAML Coonfiguration
The YAML configuration file format is used to pass parameters to the aggregation job.

*Example:*
```
general:
  appname: SparkAggregator
  window: 30 #seconds

input:
  bindaddress: 0.0.0.0
  port: <rcv_port>

output:
  hostname: <dst_hostname>
  port: <dst_port>

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

The YAML configuration file MUST follow these rules:
- The `general`, `input` and `output` sections are mandatory
- The `aggr_functs` is optional even if this situation is useless
- The keywords `default`, `avg`, `sum`, `max` and `min` are optionals but if inserted, a metricname/removetags list MUST be provided
- All key words ( `general`, `appname`, `input`, ...) MUST be lowercase
- The `window` and `port` parameters MUST be integer


In the aggregation function section the `default` keyword is used to aggregate all metrics without the need to list all of them


Tab. 1

| **Allowed functions** | Description | Returned value |
| ----------| -----------------| ------- |
| *avg* | Compute the average | Double |
| *sum* | Compute the sum     | Double |
| *max* | Extract the maximum | Double | 
| *min* | Extract the minimum | Double |


Tab. 2

|  Section  |  First Keyword   |  Second Keyword(s)   | Mandatory | Description |
| ----------| -----------------| ------- | ----------- |----------- |
| *general* | -          | -  | Yes    | Defines the start of 'general' configuration section |
| *general* | *addname*  | -  |  Yes    | Name to assign to the aggregation job. Es: "SparkAggregationJob" |
| *general* | *window*   | -  |  Yes    | Time window size, in seconds |
| *input*   | -          | -  |  Yes    | Defines the start of 'input' configuration section where the inner Avro Source parameters are defined |
| *input*   | *bind*     | -  |  Yes    | Bind address. Es "0.0.0.0" |
| *input*   | *port*     | -  |  Yes    | Port where to listen to |
| *output*  | -          | -  |  Yes    | Define the start of 'output' configuration section where the UDP transmitter parameters are defined |
| *output*  | *hostname* | -  |  Yes    | Hostname where to send data |
| *output*  | *port*     | -  |  Yes    | Port where to send data |
| *aggr_functs* | -       | -  |  No     | Defines the start of 'aggregation functionst' configuration section |
| *aggr_functs*  | *avg/sum/max/min*  | -  |  No    | One of allowed aggregation functions listed in the Tab 1. Requires a list of {*metricname* and *removetags* }|
| *aggr_functs*  | *avg/sum/max/min*  | *metricname*  |  Yes (if the parent *allowed_function* is present) | Metric name to aggregate |
| *aggr_functs*  | *avg/sum/max/min*  | *removetags*  |  Yes (if the parent *allowed_function* is present) | Tags to remove during the aggregation phase. If you want a global value for all host, just type [tag_host]|
| *aggr_functs*  | *defaut*  | -  |  No    | Defines the default aggregation function and the tags to remove |
| *aggr_functs*  | *defaut*  | *function*  |  Yes (if *default* is present) | Function to use in the aggregation. Only the function listed in the Tab 1 are allowed |
| *aggr_functs*  | *defaut*  | *removetags*  |  Yes (if *default* is present) | Tags to remove during the aggregation phase. If you want a global value for all host, just type [tag_host]|

## Input format
The input format MUST have the followings fields:

| Field | Mandatory | Description |
| ------| -----------------| ------- |
| *name* | Yes | Metric name |
| *value_value* | Yes | Metric value | 
| *tag_* | No | All tags must begin with "tag_". Es "tag_host", "tag_nic" |
| *timestamp* | No | Timestamp |

All fields are strings.

## Output format
The output format is that used in the [Flume UDP Source](https://github.com/AliceO2Group/MonitoringCustomComponents/tree/master/flume-udp-source):


```JSON
{ "headers": 
  {
    "name": "<initial_metric_name>_aggr",
    "timestamp": "1528729384000000000",
    "value_<used_aggregation_function>": "1251533299.265",
    "tag_host": "o2.cern.ch",
    "tag_path": "/tmp",
    "type_value": "double"
  },
  "body" : ""
}
  
For example:

```JSON
{ "headers": 
  {
    "name": "disk_usage_aggr",
    "timestamp": "1528729384000000000",
    "value_avg": "1251533299.265",
    "tag_host": "o2.cern.ch",
    "tag_path": "/tmp",
    "type_value": "double"
  },
  "body" : ""
}

All fields are strings.