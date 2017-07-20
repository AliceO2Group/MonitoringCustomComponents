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
| mode           | event   | Output mode: `event` or `pass` |


*Example:*
 ~~~
 a1.sources = udp
 a1.sources.udp.type = ch.cern.alice.o2.flume.UDPSource
 a1.sources.udp.port = 1234
 a1.sources.udp.mode = event
 ~~~
3. Start Flume agent
 ~~~
 ./bin/flume-ng agent -n a1 -c conf -f conf/o2.conf
 ~~~
