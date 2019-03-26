# Flume Custom InfluxDB Timestamp Interceptor

## Build
1. Clone repository
```
git clone https://github.com/AliceO2Group/MonitoringCustomComponents.git
cd MonitoringCustomComponents/flume-influxdb-timestamp-interceptor
```

2. Compile
```
mvn clean -e install -DskipTests
```

## Run
1. Move created `.jar` file from `target/` to Flume's `lib/` directory

2. Configure the agent including the UDP Influxdb sink

| Property name  | Default | Description |
| -------------- | ------- | ----------- |
| *type*         | -       | Must be set to `ch.cern.alice.o2.flume.InfluxDbTimestampInterceptor$Builder` |
| *timestampTag* | timestamp | Tag name |

*Example:*
 ```java
# Sources
agent.sources = s1
agent.sources.s1.channels = channel_mem
agent.sources.s1.interceptors = i1
agent.sources.s1.interceptors.i1.type = ch.cern.alice.o2.flume.InfluxDbTimestampInterceptor$Builder
agent.sources.s1.interceptors.i1.timestampTag = flumeTimestamp
	
# Channels
agent.channels = channel_mem
agent.channels.channel_mem.type = memory
 	
# Sinks
agent.sinks = avro
agent.sinks.avro.channel = channel_mem
```

3. Start Flume agent
```
export $FLUME_HOME=<FLUME_ROOT_DIR>
$FLUME_HOME/bin/flume-ng agent --conf-file $FLUME_HOME/conf/o2.conf --name agent --conf $FLUME_HOME/conf/
```
