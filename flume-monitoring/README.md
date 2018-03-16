# Flume Monitoring Component

This component monitors multiple flume agents formatting the information using the Influxdb Line Protocol format.
Executing the script in Telegraf, it acts like a plug-in to monitor Flume agents.


## Configure

1. Run the flume agent(s) with `-Dflume.monitoring.type` and `-Dflume.monitoring.port` options in order to export the monitoring data related to that component.


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

The script is able to monitor multiple flume agents by reading the monitoring data exported from each component on `<host:port>`.

There is the possibility to assign to the flume agent a custom name using the `<agent_name>` field.

Multiple flume agents can be monitored providing a comma seratated list, as shown in the example

*Example:*

~~~
python $SCRIPTPATH/py_flume.py --agents "host1:post1=name1,host2:port2=name2"
~~~
