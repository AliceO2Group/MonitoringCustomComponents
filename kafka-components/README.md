
# Kafka components
<<<<<<< HEAD
This folder contains all components used with [Apache Kafka](https://kafka.apache.org) in order to provide aggregation, processing and forwarding functionalities. 

Current components:
- [InfluxDB UDP Consumer]()
- [Mattermost Consumer]()

## Dependencies
- Java > 1.8
- Apache Kafka >= 2.0
	
## Build
=======
This directory contains [Apache Kafka](https://kafka.apache.org) custom components in order to collect, process, aggregate, and cosume metrics. 

Functional components:
- [InfluxDB UDP Consumer](#influxdb-udp-consumer)
- [Mattermost Consumer](#mattermost-consumer)

### Dependencies
- Java > 1.8

### Build
>>>>>>> 3afd732d0bd45251f93ab3e128e0ea1b9e63568e
1. Clone repository
```
 git clone https://github.com/AliceO2Group/MonitoringCustomComponents.git && cd kafka-components 
```
2. Compile
```
 mvn clean -e install -DskipTests 
```

<<<<<<< HEAD
The generated jar (`.target/o2-kafka-0.1-jar-with-dependencies.jar`) contains all dependencies inside, so there is no need to download them manually.

## Run

1. Edit the YAML configuration file according the specific component
2. Execute the command
=======
The generated jar (`target/o2-kafka-0.1-jar-with-dependencies.jar`) includes all components and dependencies.
>>>>>>> 3afd732d0bd45251f93ab3e128e0ea1b9e63568e

## Components

### InfluxDB UDP Consumer
<<<<<<< HEAD
This component retrieves messages from the Kafka cluster and forward them to a single InfluxDB instance. 
The messages are supposed have the [Line Protocol format](https://docs.influxdata.com/influxdb/v1.7/write_protocols/line_protocol_reference/).
The component could be configured in order to send inner monitoring data to an InfluxDB instance.

#### Command
The consumer is execute using the following command
=======
This component retrieves messages from the Kafka cluster and forward them to an InfluxDB instance. 
The messages need to be formatted in the [Line Protocol format](https://docs.influxdata.com/influxdb/v1.7/write_protocols/line_protocol_reference/).
The component could be configured in order to send inner monitoring data to an InfluxDB instance.

#### Run
The consumer can started using the following command:

```
java -cp target/o2-kafka-0.1-jar-with-dependencies.jar \
 ch.cern.alice.o2.kafka.connectors.InfluxdbUdpConsumer \
 --config conf-influxdb-udp.yaml
```

#### Configuration file 
A configuration file example is

```
general:
   log4jfilename: ./log4j.properties

kafka_consumer_config:
   bootstrap.servers: broker1:9092,broker2:9092,broker3:9092
   topic: input-topic
   group.id: influxdb-udp-consumer
   auto.offset.reset: latest
   fetch.min.bytes: 1
   receive.buffer.bytes: 262144
   max.poll.records: 1000000


sender_config:
   hostname: <influxdb-instance-hostname>
   port: 8089

stats_config:
   enabled: true
   hostname: <infludb-instance-hostname>
   port: 8090
   period_ms: 10000
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

The above JSON is converted in a Mattermost message with a JSON field per line. 
Only the `description` field is mandatory, the remaining two ones are printed if present.

 
The component could be configured in order to send inner monitoring data to an InfluxDB instance.

#### Command
The consumer is execute using the following command

```
java -cp target/o2-kafka-0.1-jar-with-dependencies.jar \
  ch.cern.alice.o2.kafka.connectors.MattermostConsumer  \
  --config conf-mattermost.yaml
```

#### Configuration file 
A configuration file example is

```
general:
   log4jfilename: ./log4j.properties

kafka_consumer_config:
   bootstrap.servers: broker1:9092,broker2:9092,broker3:9092
   topic: notification-topic

mattermostr_config:
   url: https://<mattermost-server-hostname>/hooks/<token-id>

stats_config:
   enabled: true
   hostname: <infludb-instance-hostname>
   port: 8090
   period_ms: 10000
```

Tab. 2

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
| *mattermost* | - | Yes | Defines the start of 'mattermost' configuration section | 
| *mattermost* | *url* | Yes | Mattermost url https://<mattermost-server-hostname>/hooks/<token-id> | 
| *stats* | - | Yes | Defines the start of 'stats' configuration section | 
| *stats* | *enabled* | Yes | Set `true` to enable the self-monitoring functionality | 
| *stats*  | *hostname* | No | Endpoint hostname | 
| *stats*  | *port*   | No | Endpoint port |
| *stats*  | *period_ms* | No | Statistic report period |

### Email Consumer
This component retrieves messages from the Kafka cluster and send email.
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
The consumer is execute using the following command

```
java -cp target/o2-kafka-0.1-jar-with-dependencies.jar \
  ch.cern.alice.o2.kafka.connectors.EmailConsumer  \
  --config conf-email.yaml
```

#### Configuration file 
A configuration file example is

```
general:
   log4jfilename: ./log4j-email.properties

kafka_consumer_config:
   bootstrap.servers: broker1:9092,broker2:9092,broker3:9092
   topic: email-topic

email_config:
   hostname: <smtp-server>
   port: <smtp-port>
   from: <from-email-aggress>
   username: <username>
   password: <password>

stats_config:
   enabled: true
   hostname: <infludb-instance-hostname>
   port: 8090
   period_ms: 10000
```

Tab. 2

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

