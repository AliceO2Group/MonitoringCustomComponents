# Flume Monitoring
This component monitors multiple Flume agents and formats the information using the InfluxDB Line Protocol.
It pushes the data to InfluxDB via Telegraf.


## Configure
1. Run the Flume agent(s) with `-Dflume.monitoring.type` and `-Dflume.monitoring.port` options.

*Example:*
~~~
 /bin/bash $FLUME_HOME/bin/flume-ng agent -n <agent_name> -c $FLUME_HOME/conf -f $FLUME_HOME/conf/flume.properties -Dflume.monitoring.type=http -Dflume.monitoring.port=<port>
~~~

2. Configure Telegraf
Configure Telegraf in order to execute the bash script
~~~
[[inputs.exec]]
    commands = [ "/path/to/getFlumeMonitoringData.sh" ]
~~~

3. Run getFlumeMonitoringData.sh
Pass as parameter `<hostname>`:`<port>`=<agentName>` for each agent. To define multiple agents use `,` as separator.

*Example:*
~~~
python $SCRIPTPATH/py_flume.py --agents "<hostname>=<agentName>,<hostname>=<agentName>"
~~~
