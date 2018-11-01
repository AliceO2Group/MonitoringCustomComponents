#!/bin/bash
HOSTNAME="${COLLECTD_HOSTNAME:-localhost}"
INTERVAL=60 #"${COLLECTD_INTERVAL:-60}"

while sleep "$INTERVAL"; do
  for dir in /sys/class/net/*/
  do
    res=$(<${dir}/operstate)
    if [ "$res" = "up" ]; then res=1; else res=0; fi
    dir="${dir%*/}"
    echo "PUTVAL \"${HOSTNAME}/interface_status/gauge-${dir##*/}\" interval=$INTERVAL N:${res}"
  done
done
