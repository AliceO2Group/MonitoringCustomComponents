# Flume/Spark Monitoring
This component is able to monitor multiple Flume agents and multiple running Spark applications.
The gathered data is sent to InfluxDB, via UDP.


## Flume/Spark Configuration
### Flume Configuration
Run the Flume agent(s) with `-Dflume.monitoring.type` and `-Dflume.monitoring.port` options.

*Example:*
~~~
 /bin/bash $FLUME_HOME/bin/flume-ng agent -n <agent_name> -c $FLUME_HOME/conf -f $FLUME_HOME/conf/flume.properties -Dflume.monitoring.type=http -Dflume.monitoring.port=<port>
~~~

### Spark Configuration
Start spark job. Example:
~~~
$SPARK_HOME/bin/spark-submit --class ch.cern.spark.streaming.SparkAggregator --master local[*] /path/to/jar
~~~

## Module configuration
The Configuration file is in YAML format. 
In the log section is possible to configure the logfile path and the log level. It's identificated using the keywork `log`
~~~
```
log:
   logfile: /tmp/sensor.log
   level: INFO

pidfile: /tmp/sensor.pid
   

sched:
   period: 10 #seconds

input:
  - name: flume
    conf:
       endpoints:
         - endpoint: "127.0.0.1:5653"
           name: "flume-agent1"
         - endpoint: "127.0.0.1:5654"
           name: "flume-agent2"
  - name: spark
    conf:
       endpoints:
         - endpoint: "127.0.0.1:4040"

output:
  - name: influxdb-udp
    conf: 
       endpoint: "127.0.0.1"
       port : 8089
```
~~~

## Run command

```
python /path/to/Sensor.py -conf /path/to/conf/conf.yaml [cmd]

```

| cmd  | Description |
| ---------| ----------- |
| *start*  | start the daemon |
| *stop*   | stop the daemon |
| *restart* | restart the daemon |
| *debug* | print in console the gathered data |
