#!/bin/bash
LINES=`sudo source /etc/profile.d/modules.sh && MODULEPATH=/root/alibuild.flp/sw/MODULES/slc7_x86-64 module load ReadoutCard/latest 2>/dev/null && /root/alibuild.flp/sw/slc7_x86-64/ReadoutCard/latest/bin/roc-metrics --id 0`
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
