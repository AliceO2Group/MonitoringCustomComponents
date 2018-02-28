# Flume UDP InfluxDB Sink

## Build
1) Install Apache Maven - https://maven.apache.org/install.html

2) Clone repository
 ~~~
 git clone https://github.com/AliceO2Group/MonitoringCustomComponents.git
 cd MonitoringCustomComponents/flume-udp-influxdb-sink
 ~~~
3) Compile
 ~~~
 mvn clean -e install
 ~~~

## Run
1) Move created `.jar` file from `target/` to Flume's `lib/` directory

2) Configure the agent including the UDP Influxdb sink

| Property name  | Default | Description |
| -------------- | ------- | ----------- |
| *type*         | -       | Must be set to `ch.cern.alice.o2.flume.InfluxDbUdpSink` |
| *hostname*     | -       | InfluxDB hostname |
| *port*         | -       | InfluxDB UDP port number |
| mode           | pass    | Output mode: `event` or `pass` |

*Example:*
 ~~~
 # Sources
 agent.sources = my_source
 agent.sources.my_source.channels = channel_mem
	
 # Channels
 agent.channels = channel_mem
 agent.channels.channel_mem.type = memory
 agent.channels.channel_mem.capacity = 1000
 agent.channels.channel_mem.transactionCapacity = 1000
	
 # Sinks
 agent.sinks = sink_influxdb
 agent.sinks.sink_influxdb.type = ch.cern.alice.o2.flume.InfluxDbUdpSink
 agent.sinks.sink_influxdb.channel = channel_mem
 agent.sinks.sink_influxdb.hostname = <ip>
 agent.sinks.sink_influxdb.port = <port>
 agent.sinks.sink_influxdb.mode = pass
 ~~~

3) Start Flume agent
 ~~~
 $FLUME_HOME/bin/flume-ng agent --conf-file $FLUME_HOME/conf/o2.conf --name agent --conf $FLUME_HOME/conf/
 ~~~

## Event format
If `event` is selected, the metric information must be present entirely in the event header. The event body is not read.
Some fields must be present to validate the event
Headers:

| Key            | Description |
| -------------- | ----------- |
| *name*         | metric name |
| *value_\**     | At least one value field whose name start with `value_`. Example `value_reads` |


Optional fields

| Key            | Description |
| -------------- | ----------- |
| *tag_\**       | Tags can be added using field whose name start with `tag_`. Example `tag_host` |
| timestamp      | UnixTimestamp format in nanoseconds: Example: 1519751192000000000 |


Example:

| Key                | Description |
| ------------------ | ----------- |
| name               | cpu         |
| tag_host           | host1       |
| tag_cpu            | cpu1        |
| value_usage_idle   | 0.98        |
| value_usage_system | 0.01        |
| value_usage_user   | 0.01        |
| timestamp          | 1519751192000000000 |


Key Name different from which showed won't be sent to InfluxDB
