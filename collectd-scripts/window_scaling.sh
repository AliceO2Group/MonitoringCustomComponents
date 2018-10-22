#!/bin/bash
HOSTNAME="${COLLECTD_HOSTNAME:-localhost}"
INTERVAL=60 #"${COLLECTD_INTERVAL:-60}"

while sleep "$INTERVAL"; do
  res=$(</proc/sys/net/ipv4/tcp_window_scaling)
  echo "PUTVAL \"${HOSTNAME}/tcp_window_scaling/gauge-max\" interval=$INTERVAL N:${res}"
done
