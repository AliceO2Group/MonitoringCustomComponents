# Monitoring Custom Components
This repository consists of monitoring custom components to: [Apache Flume](https://flume.apache.org) and [Apache Spark](https://spark.apache.org).

## Requiremets
In order to build a component Java and Apache Maven is required.

#### macOS
```
brew install maven
```

#### CC7 (and other Linux distributions)
1. Go to [Apache Maven download page](https://maven.apache.org/download.cgi)
2. Download binary archive (eg. [apache-maven-3.5.3-bin.tar.gz](http://www-eu.apache.org/dist/maven/maven-3/3.5.3/binaries/apache-maven-3.5.3-bin.tar.gz))
3. Unarchive and add `bin` directory to `$PATH`

## List of components
+ [flume-influxdb-timestamp-interceptor](flume-influxdb-timestamp-interceptor)
+ [flume-json-collectd-http-handler](flume-json-collectd-http-handler)
+ [flume-http-influxdb-sink](flume-http-influxdb-sink)
+ [flume-udp-influxdb-sink](flume-udp-influxdb-sink)
+ [flume-udp-source](flume-udp-source)
+ [spark-streaming-aggregator](spark-streaming-aggregator)
+ [spark-streaming-pass-through](spark-streaming-pass-through)
