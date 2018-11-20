#!/bin/bash
HOSTNAME="${COLLECTD_HOSTNAME:-localhost}"
INTERVAL=60 #"${COLLECTD_INTERVAL:-60}"

while sleep "$INTERVAL"; do
  res=$(</proc/sys/net/ipv4/tcp_rmem)
  resArray=($res)
  echo "PUTVAL \"${HOSTNAME}/tcp_rmem/gauge-min\" interval=$INTERVAL N:${resArray[0]}"
  echo "PUTVAL \"${HOSTNAME}/tcp_rmem/gauge-default\" interval=$INTERVAL N:${resArray[1]}"
  echo "PUTVAL \"${HOSTNAME}/tcp_rmem/gauge-max\" interval=$INTERVAL N:${resArray[2]}"
done
