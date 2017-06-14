# Flume UDP InfluxDB Sink

## Build
1) Install Apache Maven - https://maven.apache.org/install.html

2) Clone repository

```
     git clone https://github.com/AliceO2Group/MonitroingCustomComponents.git && cd flume-udp-influxdb-sink
```
3) Compile
```
     mvn clean -e install
```

## Run
1) Move created `.jar` file from `target/` to Flume's `lib/` directory

2) Configure the agent including the UDP Influxdb sink. Example:
```
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
    agent.sinks.sink_influxdb.type = ch.cern.alice.o2.flume.InfluxdbUDPSink
    agent.sinks.sink_influxdb.channel = channel_mem
    agent.sinks.sink_influxdb.hostname = <ip>
    agent.sinks.sink_influxdb.port = <port>
```

3) Start Flume agent


```
   $FLUME_HOME/bin/flume-ng agent --conf-file $FLUME_HOME/conf/o2.conf --name agent --conf $FLUME_HOME/conf/
```
   
