#!/bin/bash
HOSTNAME="${COLLECTD_HOSTNAME:-localhost}"
INTERVAL=60 #"${COLLECTD_INTERVAL:-60}"

while sleep "$INTERVAL"; do
  for dir in /sys/class/net/*/
  do
    res=$(<${dir}/mtu)
    dir="${dir%*/}"
    echo "PUTVAL \"${HOSTNAME}/interface_mtu/gauge-${dir##*/}\" interval=$INTERVAL N:${res}"
  done
done
