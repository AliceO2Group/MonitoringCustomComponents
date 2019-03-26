#!/bin/bash
source /etc/profile.d/modules.sh
MODULEPATH=/opt/alisw/el7/modulefiles module load ReadoutCard/v0.9.0-1

LINES=`/opt/alisw/el7/ReadoutCard/v0.9.0-1/bin/roc-metrics`
SHOW=0
while read -r line; do
  if [ "${line:0:3}" = "===" ]; then
    SHOW=0
  fi
  if [ "$SHOW" -eq "1" ]; then
    lineArray=($line)
    echo "cru,pcie=${lineArray[2]} temperature=${lineArray[3]},dropped=${lineArray[4]},clock=${lineArray[6]},links=${lineArray[7]}"
  fi
  if [ "${line:0:3}" = "---" ]; then
    SHOW=1
  fi
done <<< "$LINES"
