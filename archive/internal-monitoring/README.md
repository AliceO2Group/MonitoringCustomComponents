# Flume and Spark internal monitoring
This component monitors Flume agents and Spark jobs. The gathered internal metrics are sent to InfluxDB over UDP.


## Configuration
### Flume configuration
Run the Flume agent(s) with additional options: `-Dflume.monitoring.type` and `-Dflume.monitoring.port`, e.g.:
~~~
$FLUME_HOME/bin/flume-ng agent -n <agent_name> -c $FLUME_HOME/conf -f $FLUME_HOME/conf/flume.properties -Dflume.monitoring.type=http -Dflume.monitoring.port=<port>
~~~

### Spark configuration
Start spark job as usual, e.g.:
~~~
$SPARK_HOME/bin/spark-submit --class ch.cern.spark.streaming.SparkAggregator --master local[*] </path/to/jar>
~~~

## Module configuration
The configuration file is in YAML format. The file content in self-explanatory.
~~~
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
~~~

## Start module
```
python </path/to/>Sensor.py -conf </path/to/conf/>conf.yaml <cmd>
```
Where `<cmd>`:
 - *start* - start the daemon
 - *stop* - stop the daemon
 - *restart* - restart the daemon
 - *debug* - print in console the gathered data
