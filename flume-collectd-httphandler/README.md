# Flume JSON Collectd HTTP Handler

## Version

This component version has been tested with Apache flume 1.8.0

## Build
1) Install Apache Maven - https://maven.apache.org/install.html

2) Clone repository
 ~~~
 git clone https://github.com/AliceO2Group/MonitoringCustomComponents.git
 cd MonitoringCustomComponents/flume-collectd-httphandler
 ~~~
3) Compile
 ~~~
 mvn clean -e install -DskipTests
 ~~~

## Run
1) Move created `.jar` file from `target/` to Flume's `lib/` directory

2) Configure the agent including the Flume HTTP Source
   Use the JSONCollectdHTTPHandler handler in the source configuration in order to
   convert the JSON Collectd HTTP requests in Flume event(s).

| Property name  | Default | Description |
| -------------- | ------- | ----------- |
| *type*         | -       | Must be set to `http` |
| *bind*         | -       | The hostname or IP address to listen on |
| *port*         | -       | The port the HTTP Source should bind to. |
| *handler*      | -       | Must be set to `ch.cern.alice.o2.flume.JSONCollectdHTTPHandler` |

*Example:*
 ~~~
 # Sources
 agent.sources = source_http  
 agent.sources.source_http.type = http
 agent.sources.source_http.port = <port>
 agent.sources.source_http.bind = 0.0.0.0
 agent.sources.source_http.handler = ch.cern.alice.o2.flume.JSONCollectdHTTPHandler
 agent.sources.source_http.channels = mem_ch

 # Channels
 agent.channels = mem_ch
 agent.channels.mem_ch.type = memory
 agent.channels.mem_ch.capacity = 1000

 # InfluxDB UDP sink
 agent.sinks = sink
 agent.sinks.sink.channel = mem_ch
 agent.sinks.sink.type = <sink_type>
 ~~~

3) Start Flume agent
 ~~~
 $FLUME_HOME/bin/flume-ng agent --conf-file $FLUME_HOME/conf/o2.conf --name agent --conf $FLUME_HOME/conf/ -Dflume.monitoring.type=http -Dflume.monitoring.port=5653
 ~~~

## Events format
A single JSON Collectd metric has this structure:

~~~
{"values":24125440,24576],"dstypes":"derive","derive"],"dsnames":"read","write"],"time":1520605461.795,"interval":10.000,
"host":"aido2ssd.cern.ch","plugin":"disk","plugin_instance":"dev_sda1","type":"disk_octets","type_instance":""}
~~~

The Handler converts it in a Flume event having this structure:

~~~
{tag_instance=dev_sda1, tag_type=disk_octets, value_value=24125440, name=disk_read, type_value=long, tag_host=aido2ssd.cern.ch, timestamp=1520605477199675392}
~~~