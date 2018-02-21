#!/bin/bash
unset http_proxy
unset https_proxy

SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
PYTHON=$(  which python  )

host1=<host1>:<port1>
name1=<agent1_name>
host2=<host2>:<port2>
name2=<agent2_name>

$PYTHON $SCRIPTPATH/py_flume.py --agents "$host1=$name1,$host2=$name2"
