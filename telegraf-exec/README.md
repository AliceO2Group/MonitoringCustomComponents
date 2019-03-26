# CRU monitoring

1. Install `collectd-5.8.0-1.el7.x86_64.rpm`
2. Create a new user: `collectd`
3. Place [collecd.conf](https://raw.githubusercontent.com/AliceO2Group/MonitoringCustomComponents/master/collectd-scripts/collectd.conf) file into `/etc`
4. Create `cru.conf` file in `/etc/collectd.d` with the following content:
```
LoadPlugin exec
<Plugin exec>
  Exec "collectd:pda" "/etc/collectd.d/scripts/cru.sh"
</Plugin>

LoadPlugin network
<Plugin network>
  Server "<influx_server>" "25827"
</Plugin>
```

5. Create directory: `/etc/collectd.d/scripts`
6. Place [scripts/cru.sh](https://raw.githubusercontent.com/AliceO2Group/MonitoringCustomComponents/master/collectd-scripts/scripts/cru.sh) file into `/etc/collectd.d/scripts` and make sure it loads ReadoutCard MODULE that exists
7. Disable SELinux: `setenforce 0`
8. Start collectd: `systemctl start collectd`
