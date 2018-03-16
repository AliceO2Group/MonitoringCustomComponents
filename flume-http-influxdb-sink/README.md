# Flume UDP InfluxDB Sink

## Build
1. Clone repository

```
git clone https://github.com/AliceO2Group/MonitoringCustomComponents.git
cd MonitoringCustomComponents/flume-http-influxdb-sink
```

2. Compile
```
mvn clean -e install
```

## Run
1. Move created `.jar` file from `target/` to Flume's `lib/` directory

2. Configure the agent including the UDP Influxdb sink. Example:
```java
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
agent.sinks.sink_influxdb.type = org.apache.flume.sink.influxdb.InfluxDbHttpSink
agent.sinks.sink_influxdb.channel = channel_mem
agent.sinks.sink_influxdb.hostname = <ip>
agent.sinks.sink_influxdb.port = <port>
agent.sinks.sink_influxdb.database = <database>
agent.sinks.sink_influxdb.batchSize = 500000
```

3. Start Flume agent
```
$export $FLUME_HOME=<FLUME_ROOT_DIR>
$FLUME_HOME/ibin/flume-ng agent --conf-file $FLUME_HOME/conf/o2.conf --name agent --conf $FLUME_HOME/conf/
```
   
