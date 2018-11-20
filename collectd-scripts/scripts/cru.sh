#!/bin/bash
source /etc/profile.d/modules.sh
MODULEPATH=/opt/alisw/el7/modulefiles module load ReadoutCard/v0.9.0-1

HOSTNAME="${COLLECTD_HOSTNAME:-localhost}"
INTERVAL="${COLLECTD_INTERVAL:-15}"

while sleep "$INTERVAL"; do
  LINES=`/opt/alisw/el7/ReadoutCard/v0.9.0-1/bin/roc-metrics`
  SHOW=0
  while read -r line; do
    if [ "${line:0:3}" = "===" ]; then
      SHOW=0
    fi
    if [ "$SHOW" -eq "1" ]; then
      lineArray=($line)
      echo "PUTVAL \"${HOSTNAME}/cru_temperature/gauge-${lineArray[2]}\" interval=$INTERVAL N:${lineArray[3]}"
      echo "PUTVAL \"${HOSTNAME}/cru_dropped/gauge-${lineArray[2]}\" interval=$INTERVAL N:${lineArray[4]}"
      echo "PUTVAL \"${HOSTNAME}/cru_clock/gauge-${lineArray[2]}\" interval=$INTERVAL N:${lineArray[6]}"
      echo "PUTVAL \"${HOSTNAME}/cru_links/gauge-${lineArray[2]}\" interval=$INTERVAL N:${lineArray[7]}"
    fi
    if [ "${line:0:3}" = "---" ]; then
      SHOW=1
    fi
  done <<< "$LINES"
done
