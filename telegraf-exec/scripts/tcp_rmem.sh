#!/bin/bash
res=$(</proc/sys/net/ipv4/tcp_rmem)
resArray=($res)
echo "tcp_rmem min=${resArray[0]},default=${resArray[1]},max=${resArray[2]}"
