# Flume UDP/JSON Source

## Build
1. Install Apache Maven - https://maven.apache.org/install.html
2. Clone repository
 ~~~
 git clone https://github.com/AliceO2Group/MonitroingCustomComponents.git && cd flume-udp-source
 ~~~
3. Compile
 ~~~
 mvn clean package -e
 ~~~
## Run
1. Move created `.jar` file from `target/` to Flume's `lib/` directory
2. Configure UDP source (see [config/o2.conf](config/o2.conf))

| Property name  | Default | Description |
| -------------- | ------- | ----------- |
| *type*         | -       | Must be set to `ch.cern.alice.o2.flume.UDPSource` |
| *port*         | -       | UDP port number |


*Example:*
 ~~~
 a1.sources = udp
 a1.sources.udp.type = ch.cern.alice.o2.flume.UDPSource
 a1.sources.udp.port = 1234
 ~~~
3. Start Flume agent
 ~~~
 ./bin/flume-ng agent -n a1 -c conf -f conf/o2.conf
 ~~~

## Counters
The following counters are available:
+ `EventAcceptedCount` - number of successfully received and parsed metrics

To enable monitoring counters and publish them via HTTP following parameters needs to be added to start up command:
~~~
-Dflume.monitoring.type=http -Dflume.monitoring.port=<port>
~~~

Counters are published at the following URL:
~~~
http://<host>:<port>/metrics
~~~
