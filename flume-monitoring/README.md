# Flume Monitoring Component

This component monitors multiple flume agents formatting the information using the Influxdb Line Protocol format.
Executing the script in Telegraf, it acts like a plug-in to monitor Flume agents.


## Configure

1. Run the flume agent(s) with `-Dflume.monitoring.type` and `-Dflume.monitoring.port` options.


*Example:*
~~~
 /bin/bash $FLUME_HOME/bin/flume-ng agent -n <agent_name> -c $FLUME_HOME/conf -f $FLUME_HOME/conf/flume.properties -Dflume.monitoring.type=http -Dflume.monitoring.port=<port> &
~~~
2. Configure Telegraf


Configure Telegraf in order to execute the bash script
~~~
[[inputs.exec]]
    commands = [ "/path/to/getFlumeMonitoringData.sh" ]
~~~
3. Configure getFlumeMonitoringData.sh


Add multiple flume agent adding copple `<host>`:`<name>` separated with comma `','` 

*Example:*

~~~
host1=<host1>:<port1>
name1=<agent1_name>
host2=<host2>:<port2>
name2=<agent2_name>

python $SCRIPTPATH/py_flume.py --agents "$host1=$name1,$host2=$name2"
~~~
