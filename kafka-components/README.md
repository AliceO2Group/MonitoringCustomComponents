

# Kafka components
This directory contains [Apache Kafka](https://kafka.apache.org) custom components in order to collect, process, aggregate, and cosume metrics. 

[Utility components](#utility-components):
- [Import Records](#import-records-component)

[Processing components](#processing-components):
- [On Off](#on-off-component)
- [Aggregator](#aggregator-component)

[Consumer components](#consumer-components):
- [InfluxDB UDP Consumer](#influxdb-udp-consumer)
- [Mattermost Consumer](#mattermost-consumer)
- [Email Consumer](#email-Consumer)

### Dependencies
- Java > 1.8

### Version
`export VERSION=0.2`

### Build
1. Clone repository
```
 git clone https://github.com/AliceO2Group/MonitoringCustomComponents.git && cd kafka-components 
```
2. Compile
```
 mvn clean -e install -DskipTests 
```

The generated jar (`target/kafka-streams-o2-$VERSION-jar-with-dependencies.jar`) includes all components and dependencies.

## Utility Components
In this category belong all components used to allow the processing and consumer components to be executed.

### Import Records Component
This component converts input messages, using the InfluxDB Line Protocol, in the following internal protocol:

(Input) InfluxDB Line Protocol: `<measurement>,<tags> <field_name1>=<field_value1>,...,<field_nameN>=<field_valueN> <timestamp>`
(Output) protocol: `(key,value)`

Where:
- key: `<measurement_name>#<field_name>`
- value: `<tags>#<field_value>#<timestamp>`

Both, key and value, are strings.
The component is able to splits messagges containg multiple values in single value messages. 

#### Run
The consumer can be started using the following command:

```
java -cp target/kafka-streams-o2-$VERSION-jar-with-dependencies.jar \
 ch.cern.alice.o2.kafka.streams.ImportRecords \
 --config configs/conf-import-records.yaml
```
#### Configuration file 
A configuration file example is:

```
general:
   log4jfilename: configs/log4j-import-records.properties

kafka_config:
   bootstrap.servers: <broker1:9092,broker2:9092,broker3:9092>

import_config:
   topic.input: <input-topic>
   topic.output: <output-topic>

stats_config:
   enabled: true
   hostname: <infludb-hostname>
   port: <influxdb-port>
   period_ms: <sample-period-in-milliseconds>
```

Tab. 1

| Section | First Keyword | Mandatory | Description | Default value |
| --------| --------------| ----------| ----------- | ------------- |
| *general* | -          | Yes    | Defines the start of 'general' configuration section | - |
| *general* | *log4jfilename* | Yes | Log configuration filename | - |
| *kafka_config* | - | Yes | Defines the start of 'kafka_config' configuration section | - |
| *kafka_config* | *bootstrap.servers* | Yes | Comma separated list of the Kafka cluster brokers | - |
| *import_config* | - | Yes | Defines the start of 'import_config' configuration section | 
| *import_config* | *topic.input* | Yes | Topic where reads InfluxDB Line Protocol messages | 
| *import_config* | *topic.output* | Yes | Topic where writes messages using the internal procotol | 
| *stats* | - | Yes | Defines the start of 'stats' configuration section | 
| *stats* | *enabled* | Yes | Set `true` to enable the self-monitoring functionality | 
| *stats*  | *hostname* | No | Endpoint hostname | 
| *stats*  | *port*   | No | Endpoint port |
| *stats*  | *period_ms* | No | Statistic report period |

## Processing Components
Kafka components able to extract aggregated values starting from raw data.

### On Off Component
This component extracts messages whose value is changed respect the last stored value and forwards them to an output topic. Moreover, periodically it sends all stored values to the output topic. The component provides the possibility to select the pair `(measurement,field_name)` where apply the change detector.
The component is able to read only internal format messages generated using the [Import Records](#import-records-component).

#### Run
The consumer can be started using the following command:

```
java -cp target/kafka-streams-o2-$VERSION-jar-with-dependencies.jar \
 ch.cern.alice.o2.kafka.streams.ChangeDetector \
 --config configs/conf-change-detector.yaml
```
#### Configuration file 
A configuration file example is:

```
general:
   log4jfilename: configs/log4j-change-detector.properties

kafka_config:
   bootstrap.servers: <broker1:9092,broker2:9092,broker3:9092>

detector:
   topic.input: <input-topic>
   topic.output: <output-topic>
   refresh.period.s: <period-in-seconds>

filter:
   <measurement_0>: <field_name1>
   <measurement_1>: <field_name1>
   <measurement_1>: <field_name3>
   <measurement_2>: <field_name1>
   <measurement_2>: <field_name0>
   <measurement_4>: <field_name0>
   <measurement_6>: <field_name4>
   <measurement_3>: <field_name2>
   
stats_config:
   enabled: true
   hostname: <infludb-hostname>
   port: <influxdb-port>
   period_ms: <sample-period-in-milliseconds>
```

Tab. 2

| Section | First Keyword | Mandatory | Description | Default value |
| --------| --------------| ----------| ----------- | ------------- |
| *general* | -          | Yes    | Defines the start of 'general' configuration section | - |
| *general* | *log4jfilename* | Yes | Log configuration filename | - |
| *kafka_config* | - | Yes | Defines the start of 'kafka_config' configuration section | - |
| *kafka_config* | *bootstrap.servers* | Yes | Comma separated list of the Kafka cluster brokers | - |
| *detector* | - | Yes | Defines the start of 'detector' configuration section | 
| *detector* | *topic.input* | Yes | Topic where to read messages | 
| *detector* | *topic.output* | Yes | Topic where to write detected messages | 
| *detector* | *refresh.period.s* | No | Period, in seconds, used from the periodical sender to forward stored values to the output topic | 
| * filter* | - | Yes | Defines the start of 'filter' configuration section | 
| * filter* | `<measurement_name>:<field_name>` | Yes | Pair `(measurement,field_name)` where apply the on off component | 
| *stats* | - | Yes | Defines the start of 'stats' configuration section | 
| *stats* | *enabled* | Yes | Set `true` to enable the self-monitoring functionality | 
| *stats*  | *hostname* | No | Endpoint hostname | 
| *stats*  | *port*   | No | Endpoint port |
| *stats*  | *period_ms* | No | Statistic report period |

### Aggregator Component
The aggregation component process the input messages using the following four functions:
- average
- sum
- minimum
- maximum

The component is able to read only internal format messages generated using the [Import Records](#import-records-component) and considers only numberic fields.

#### Command
The aggregation component can be started using the following commands:

```
java -cp target/kafka-streams-o2-$VERSION-jar-with-dependencies.jar \
  ch.cern.alice.o2.kafka.streams.Aggregator \
  --config configs/conf-aggr.yaml
```
#### Configuration file 
A configuration file example is:

```
general:
   log4jfilename: configs/log4j-aggregator.properties

kafka_config:
   bootstrap.servers: <broker1:9092,broker2:9092,broker3:9092>
   state.dir: <path-to-the-state-directory>

aggregation_config:
   window.s: <period-in-seconds>
   topic.input: <input-topic>
   topic.output: <output-topic>

avg_filter:
   -   measurement: <measurement_name0>
       field.name: <field_name0>
       tags.remove: <tag_name0>
   -   measurement: <measurement_name1>
       field.name: <field_name1>,<field_name2>
       tags.remove: <tag_name1>

sum_filter:
   -   measurement: <measurement_name3>
       field.name: <field_name3>
       tags.remove: <tag_name3>,<tag_name4>
   -   measurement: <measurement_name5>
       field.name: <field_name5>

min_filter:
   -   measurement: <measurement_name0>
       field.name: <field_name0>
       tags.remove: <tag_name0>
   -   measurement: <measurement_name1>
       field.name: <field_name1>,<field_name2>
       tags.remove: <tag_name1>

max_filter:
   -   measurement: <measurement_name3>
       field.name: <field_name3>
       tags.remove: <tag_name3>,<tag_name4>
   -   measurement: <measurement_name5>
       field.name: <field_name5>

stats_config:
   enabled: true
   hostname: <infludb-hostname>
   port: <influxdb-port>
   period_ms: <sample-period-in-milliseconds>
```

Tab. 4

| Section | First Keyword | Mandatory | Description | Default value |
| --------| --------------| ----------| ----------- | ------------- |
| *general* | -          | Yes    | Defines the start of 'general' configuration section | - |
| *general* | *log4jfilename* | Yes | Log configuration filename | - |
| *kafka_config* | - | Yes | Defines the start of 'kafka_consumer' configuration section | - |
| *kafka_config* | *bootstrap.servers* | Yes | Comma separated list of the Kafka cluster brokers | - |
| *kafka_config* | *state.dir* | Yes | Directory where store the aggregation status | - |
| *aggregation_config* | - | Yes | Defines the start of 'aggregation_config' configuration section | 
| *aggregation_config* | *window_s* | Yes | Window time in seconds | 
| *aggregation_config*  | *topic.input* | Yes | Topic where retrieve messages | 
| *aggregation_config*  | *topic.output* | Yes | Topic where sent the aggregate values |
| * avg/sum/min/max filter* | - | Yes | Defines the start of 'filter' configuration section | 
| * avg/sum/min/max filter * | `<measurement>` | Yes | measurement name where apply the aggregation function | 
| * avg/sum/min/max filter * | `<field.name>` | Yes | Comma separated fields to use | 
| * avg/sum/min/max filter * | `<tags.remove>` | No | Tags to remove during the aggregation phase | 
| *stats* | - | Yes | Defines the start of 'stats' configuration section | 
| *stats* | *enabled* | Yes | Set `true` to enable the self-monitoring functionality | 
| *stats*  | *hostname* | No | Endpoint hostname | 
| *stats*  | *port*   | No | Endpoint port |
| *stats*  | *period_ms* | No | Statistic report period |


## Consumer Components
Each consumer component retrieves messages from the Kafka cluster and forwards them to a specific external component.

### InfluxDB UDP Consumer
This component retrieves messages from the Kafka cluster and forward them to an InfluxDB instance. 
The messages need to be formatted in the [Line Protocol format](https://docs.influxdata.com/influxdb/v1.7/write_protocols/line_protocol_reference/).
The component could be configured in order to send inner monitoring data to an InfluxDB instance.

#### Run
The consumer can started using the following command:

```
java -cp target/kafka-streams-o2-$VERSION-jar-with-dependencies.jar \
 ch.cern.alice.o2.kafka.connectors.InfluxdbUdpConsumer \
 --config configs/conf-influxdb-udp.yaml
```

#### Configuration file 
A configuration file example is:

```
general:
   log4jfilename: configs/log4j-influxdb-udp.properties

kafka_consumer_config:
   bootstrap.servers: <broker1:9092,broker2:9092,broker3:9092>
   topic: <input-topic>
   group.id: influxdb-udp-consumer
   auto.offset.reset: latest
   fetch.min.bytes: 1
   receive.buffer.bytes: 262144
   max.poll.records: 1000000

sender_config:
   hostname: <influxdb-hostname>
   port: <influxdb-port>

stats_config:
   enabled: true
   hostname: <infludb-hostname>
   port: <influxdb-port>
   period_ms: <sample-period-in-milliseconds>
```

Tab. 1

| Section | First Keyword | Mandatory | Description | Default value |
| --------| --------------| ----------| ----------- | ------------- |
| *general* | -          | Yes    | Defines the start of 'general' configuration section | - |
| *general* | *log4jfilename* | Yes | Log configuration filename | - |
| *Kafka_consumer* | - | Yes | Defines the start of 'kafka_consumer' configuration section | - |
| *Kafka_consumer* | *bootstrap.servers* | Yes | Comma separated list of the Kafka cluster brokers | - |
| *Kafka_consumer* | *topic* | Yes | Input topic | - |
| *Kafka_consumer* | *group.id* | No | A unique string that identifies the consumer group this consumer belongs to used for load balancing purpose | infludb-udp-consumer |
| *Kafka_consumer* | *fetch.min.bytes* | No | The minimum amount of data the server should return for a fetch request | 1 |
| *Kafka_consumer* | *auto.offset.reset* | No | Policy in the case the offset in Kafka is lost: earliest/latest | latest |
| *Kafka_consumer* | *receive.buffer.bytes* | No | The size of the TCP receive buffer to use when reading data | 262144 |
| *Kafka_consumer* | *max.poll.records* | No | The maximum number of records returned in a single call | 1000000 |
| *sender* | - | Yes | Defines the start of 'sender' configuration section | 
| *sender* | *hostname* | Yes | InfluxDB instance hostname | 
| *sender* | *port* | Yes | InfluxDB instance port | 
| *stats* | - | Yes | Defines the start of 'stats' configuration section | 
| *stats* | *enabled* | Yes | Set `true` to enable the self-monitoring functionality | 
| *stats*  | *hostname* | No | Endpoint hostname | 
| *stats*  | *port*   | No | Endpoint port |
| *stats*  | *period_ms* | No | Statistic report period |

### Mattermost Consumer
This component retrieves messages from the Kafka cluster and forward them to the HTTP Mattermost endpoint. 
Before it's needed to create an [Incoming Webhooks](https://docs.mattermost.com/developer/webhooks-incoming.html) 
The retrived messages from Kafka are JSON format and compliant to the Grafana notification message format:

```JSON
{
  "description": "Test notification - Someone is testing the alert notification within grafana",
  "client_url" : "http://<grafana-instance>:3000",
  "details"    : "Triggered metrics:\nHigh value: 100.000\nHigher Value: 200.000"
}
```

The above JSON is converted in a Mattermost message where each JSON field is printed in a different line. 
Only the `description` field is mandatory, the remaining two ones are printed if present.

 
The component could be configured in order to send inner monitoring data to an InfluxDB instance.

#### Command
The consumer can started using the following command:

```
java -cp target/kafka-streams-o2-$VERSION-jar-with-dependencies.jar \
  ch.cern.alice.o2.kafka.connectors.MattermostConsumer  \
  --config configs/conf-mattermost.yaml
```

#### Configuration file 
A configuration file example is:

```
general:
   log4jfilename: configs/log4j-mattermost-consumer.properties

kafka_consumer_config:
   bootstrap.servers: <broker1:9092,broker2:9092,broker3:9092>
   topic: notification-topic

mattermostr_config:
   url: https://<mattermost-server-hostname>/hooks/<token-id>

stats_config:
   enabled: true
   hostname: <infludb-hostname>
   port: <influxdb-port>
   period_ms: <sample-period-in-milliseconds>
```

Tab. 2

| Section | First Keyword | Mandatory | Description | Default value |
| --------| --------------| ----------| ----------- | ------------- |
| *general* | -          | Yes    | Defines the start of 'general' configuration section | - |
| *general* | *log4jfilename* | Yes | Log configuration filename | - |
| *Kafka_consumer* | - | Yes | Defines the start of 'kafka_consumer' configuration section | - |
| *Kafka_consumer* | *bootstrap.servers* | Yes | Comma separated list of the Kafka cluster brokers | - |
| *Kafka_consumer* | *topic* | Yes | Input topic | - |
| *mattermost* | - | Yes | Defines the start of 'mattermost' configuration section | 
| *mattermost* | *url* | Yes | Mattermost url https://<mattermost-server-hostname>/hooks/<token-id> | 
| *stats* | - | Yes | Defines the start of 'stats' configuration section | 
| *stats* | *enabled* | Yes | Set `true` to enable the self-monitoring functionality | 
| *stats*  | *hostname* | No | Endpoint hostname | 
| *stats*  | *port*   | No | Endpoint port |
| *stats*  | *period_ms* | No | Statistic report period |

### Email Consumer
This component retrieves messages from the Kafka cluster and sends emails.
The retrived messages from Kafka are JSON format with the following structure:

```JSON
{
  "subject": "Title1",
  "body" : "Dear experts,\n there is a notification for you\n\nCheers,\nMonitoring Team",
  "to_addresses"    : "expert1@cern.ch,expert2@cern.ch"
}
```

All JSON fields are mandatory.

The component could be configured in order to send inner monitoring data to an InfluxDB instance.

#### Command
The consumer can started using the following command:

```
java -cp target/kafka-streams-o2-$VERSION-jar-with-dependencies.jar \
  ch.cern.alice.o2.kafka.connectors.EmailConsumer  \
  --config configs/conf-email.yaml
```

#### Configuration file 
A configuration file example is:

```
general:
   log4jfilename: configs/log4j-email-consumer.properties

kafka_consumer_config:
   bootstrap.servers: <broker1:9092,broker2:9092,broker3:9092>
   topic: email-topic

email_config:
   hostname: <smtp-server>
   port: <smtp-port>
   from: <from-email-aggress>
   username: <username>
   password: <password>

stats_config:
   enabled: true
   hostname: <infludb-hostname>
   port: <infludb-udp-port>
   period_ms: <sample-period-in-milliseconds>
```

Tab. 3

| Section | First Keyword | Mandatory | Description | Default value |
| --------| --------------| ----------| ----------- | ------------- |
| *general* | -          | Yes    | Defines the start of 'general' configuration section | - |
| *general* | *log4jfilename* | Yes | Log configuration filename | - |
| *Kafka_consumer* | - | Yes | Defines the start of 'kafka_consumer' configuration section | - |
| *Kafka_consumer* | *bootstrap.servers* | Yes | Comma separated list of the Kafka cluster brokers | - |
| *Kafka_consumer* | *topic* | Yes | Input topic | - |
| *email* | - | Yes | Defines the start of 'email' configuration section | 
| *email* | *hostname* | Yes | SMTP Server Hostname | 
| *email* | *port* | Yes | SMTP Server Port | 
| *email* | *from* | Yes | Notification email address | 
| *email* | *username* | Yes | Authentication - Username | 
| *email* | *password* | Yes | Authentication - Password | 
| *stats* | - | Yes | Defines the start of 'stats' configuration section | 
| *stats* | *enabled* | Yes | Set `true` to enable the self-monitoring functionality | 
| *stats*  | *hostname* | No | Endpoint hostname | 
| *stats*  | *port*   | No | Endpoint port |
| *stats*  | *period_ms* | No | Statistic report period |