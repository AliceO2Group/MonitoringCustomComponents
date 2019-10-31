

# Kafka components
This directory contains [Apache Kafka](https://kafka.apache.org) custom components in order to collect, process, aggregate, and cosume metrics. 

[Consumer components](#consumer-components):
- [InfluxDB UDP Consumer](#influxdb-udp-consumer)
- [Mattermost Consumer](#mattermost-consumer)
- [Email Consumer](#email-Consumer)

[Aggregation components](#aggregation-components):
- [Dispatcher](#dispatcher-component)
- [Aggregator](#aggregator-component)

### Dependencies
- Java > 1.8

### Build
1. Clone repository
```
 git clone https://github.com/AliceO2Group/MonitoringCustomComponents.git && cd kafka-components 
```
2. Compile
```
 mvn clean -e install -DskipTests 
```

The generated jar (`target/kafka-streams-o2-0.1-jar-with-dependencies.jar`) includes all components and dependencies.

## Consumer Components
Each consumer component retrieves messages from the Kafka cluster and forwards them to a specific external component.

### InfluxDB UDP Consumer
This component retrieves messages from the Kafka cluster and forward them to an InfluxDB instance. 
The messages need to be formatted in the [Line Protocol format](https://docs.influxdata.com/influxdb/v1.7/write_protocols/line_protocol_reference/).
The component could be configured in order to send inner monitoring data to an InfluxDB instance.

#### Run
The consumer can started using the following command:

```
java -cp target/kafka-streams-o2-0.1-jar-with-dependencies.jar \
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
java -cp target/kafka-streams-o2-0.1-jar-with-dependencies.jar \
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
java -cp target/kafka-streams-o2-0.1-jar-with-dependencies.jar \
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


## Aggregation Components
These components allow the aggregation of messages using the following four functions:
- average
- sum
- minimum
- maximum

The messages are retrieved from and sent to a Kafka cluster, of course different topics must be used.
Each aggregation function requires a dedicated topic for the processing:
- the [Dispatcher component](#dispatcher-component) forwards messages to these topics
- the [Aggregator components](#aggregator-component) process the aggregated values


The results are sent to an output topic formatted in the [Line Protocol format](https://docs.influxdata.com/influxdb/v1.7/write_protocols/line_protocol_reference/).
A measurement can be aggregated using only one aggregation function.


### Dispatcher Component
This component forwards messages towards specific topics following rules descrived in a configuration file.

#### Command
The dispatcher can started using the following command:

```
java -cp target/kafka-streams-o2-0.1-jar-with-dependencies.jar \
  ch.cern.alice.o2.kafka.streams.Dispatcher \
  --config configs/conf-disp.yaml
```

#### Configuration file 
A configuration file example is:

```
general:
   log4jfilename: configs/log4j-dispatcher.properties

kafka_config:
   bootstrap.servers: <broker1:9092,broker2:9092,broker3:9092>

topics:
   topic.input: <input-topic>
   topic.avg: <avg-topic>
   topic.sum: <sum-topic>
   topic.min: <min-topic>
   topic.max: <max-topic>
   topic.default: <default-topic>
   
selection:
   topic.avg:
      -   measurement: meas0
          removetags: hostname,cardid
      -   measurement: meas1
          removetags: hostname        
   topic.min:
      -   measurement: meas2
          removetags: hostname,cardid
      -   measurement: meas3         
   topic.max:
      -   measurement: meas4
          removetags: cardid    
   topic.sum:
      -   measurement: meas5
          removetags: hostname,cardid
```

Tab. 4

| Section | First Keyword | Mandatory | Description | Default value |
| --------| --------------| ----------| ----------- | ------------- |
| *general* | -          | Yes    | Defines the start of 'general' configuration section | - |
| *general* | *log4jfilename* | Yes | Log configuration filename | - |
| *kafka_config* | - | Yes | Defines the start of 'kafka_consumer' configuration section | - |
| *kafka_config* | *bootstrap.servers* | Yes | Comma separated list of the Kafka cluster brokers | - |
| *topics*  | - | Yes | Defines the start of 'topic' configuration section | 
| *topics*  | *topic.input*   | Yes | Topic where retrieves messages | 
| *topics*  | *topic.avg*     | Yes | Topic where all measurements typed under the selection/avg section are forwarded to | 
| *topics*  | *topic.sum*     | Yes | Topic where all measurements typed under the selection/sum section are forwarded to |
| *topics*  | *topic.min*     | Yes | Topic where all measurements typed under the selection/min section are forwarded to |
| *topics*  | *topic.max*     | Yes | Topic where all measurements typed under the selection/max section are forwarded to |
| *topics*  | *topic.default* | Yes | Topic where all remaining measurements are forwarded to |
| *selection* | - | Yes | Defines the start of 'selection' configuration section | 
| *selection* | *topic.[avg,min,max,sum]* | No | Defines the section related to the selected topic (e.g. avg,min,...) | 
| *selection* | *measurement* | Yes | Measurement name to forward | 
| *selection* | *removetags* | No | Tags to remove during the aggregation phase | 


### Aggregator Component
This component executes a specific aggregation function on messages read from the dedicated topic.
The topics used from the aggregators MUST have a single partition.

#### Command
The aggregation components can started using the following commands:

```
java -cp target/kafka-streams-o2-0.1-jar-with-dependencies.jar \
  ch.cern.alice.o2.kafka.streams.AggregatorAvg \
  --config configs/conf-aggr-avg.yaml
```

```
java -cp target/kafka-streams-o2-0.1-jar-with-dependencies.jar \
  ch.cern.alice.o2.kafka.streams.AggregatorSum \
  --config configs/conf-aggr-sum.yaml
```

```
java -cp target/kafka-streams-o2-0.1-jar-with-dependencies.jar \
  ch.cern.alice.o2.kafka.streams.AggregatorMin \
  --config configs/conf-aggr-min.yaml
```

```
java -cp target/kafka-streams-o2-0.1-jar-with-dependencies.jar \
  ch.cern.alice.o2.kafka.streams.AggregatorMax \
  --config configs/conf-aggr-max.yaml
```

#### Configuration file 
A configuration file example is:

```
general:
   log4jfilename: configs/log4j-aggregator-XXX.properties

kafka_config:
   bootstrap.servers: <broker1:9092,broker2:9092,broker3:9092>
   state.dir: <path-to-the-state-directory>
   
aggregation_config:
   window_s: <window-in-seconds>
   topic.input: <input-topic>
   topic.output: <output-topic>
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