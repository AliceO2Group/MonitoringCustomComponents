#!/usr/bin/env python

from sys import exit, argv
from time import time
import logging
import argparse
import json
import urllib2
import os
#===============================================================================
# OPTIONS
#del os.environ['http_proxy']
#del os.environ['https_proxy']

start_time = time()
currentdir = os.path.dirname(argv[0])
level_log = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
log_file = str(argv[0]).replace('py','log')
flume_hosts = [{"host": "localhost:5653", "name":"o2_influxdbsinkflume"}]
#===============================================================================

#===============================================================================
# ARGUMENT PARSER
parser = argparse.ArgumentParser()
parser.add_argument('-ll','--level-log', help='Level Logger: %s'%level_log, default='INFO',dest='levellog', choices=level_log)
parser.add_argument('-a','--agents', help='List of Flume Agent <host1>:<port1>|<name1>,<host2>:<port2>|<name2>', default='',dest='agentlist')
args = parser.parse_args()
#===============================================================================

#===============================================================================
# CONFIGURATIONs
try:
  logger = logging.getLogger('')
except:
  print "Error during the start of the logging module"
  exit(1)

abs_logfilename = os.path.join(currentdir,log_file)
h = logging.FileHandler(abs_logfilename)

# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
h.setFormatter(formatter)

# add the handlers to logger
logger.addHandler(h)

# set the log level
if 'DEBUG' == args.levellog:
  logger.setLevel(logging.DEBUG)
elif 'INFO' == args.levellog:
  logger.setLevel(logging.INFO)
elif 'WARNING' == args.levellog:
  logger.setLevel(logging.WARNING)
elif 'ERROR' == args.levellog:
  logger.setLevel(logging.ERROR)
elif 'CRITICAL' == args.levellog:
  logger.setLevel(logging.CRITICAL)
else:
  logger.setLevel(logging.INFO)

lAgents = list()
sAgents = args.agentlist
splitted_sAgents = sAgents.split(',')
if len(splitted_sAgents) > 0:
  for agent_name in splitted_sAgents:
    if '=' in agent_name: 
      lAgent_name = agent_name.split('=')
      if len(lAgent_name) == 2:
        host_port,name = agent_name.split('=')
        if ':' in host_port: 
          lHostPort = host_port.split(':')
          if len(lHostPort) == 2:
            host, port = lHostPort
            try:
              int_port = int(port)
              if int_port > 0 and int_port < 65536:
                lAgents.append({"host":host_port,"name":name})
              else:
                print "port {} must be in the interval 0-65536. EXIT".format(int_port)
                exit(0) 
            except:
              print "port {} must be an integer".format(port)
              exit(0) 
          else:
            print "{} not well written".format(host_port)
        else:
          print "{} without separator   ':'".format(host_port)
          exit(0)
      else:
         print "{} not well written".format(agent_name)
    else:
      print "agent name {} without separator '='".format(agent_name)
      exit(0) 
else:
  print "No agents. Exit"
  exit(0)  

#===============================================================================
# FLUME GATHERING SECTION
logger.info("START")
timestamp_ns = int(start_time*1000000000)
#print timestamp_ns
time1 = time()
data2send = list()
flume_stats_json = dict()
flume_stats_json["time"] = timestamp_ns
flume_active_hosts = 0
flume_active_components = 0
flume_gathered_data = 0

hdr = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:54.0) Gecko/20100101 Firefox/54.0',
       'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
       'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
       'Accept-Encoding': 'gzip, deflate',
       'Accept-Language': 'en-US,en;q=0.5',
       'Upgrade-Insecure-Requests' : '1',
       'Connection': 'keep-alive'}
hdr = {'User-Agent': 'Mozilla/5.0'}
for host in lAgents:
  print host
  try:
    url = "http://{}/metrics".format(host["host"])
    req = urllib2.Request(url, headers=hdr) 
    con = urllib2.urlopen( req )
    #print con.read()
    result = json.loads(con.read())
    #print result
  except:
    logger.warning("Impossible retrieve flume stats data from host: {0}".format(host["host"]))
    continue
  else:
    for item in result.keys():
      #print item
      item_key = item.split('.')[1]
      dMeas = dict()
      dMeas["tags"] = dict()
      dMeas["time"] = timestamp_ns
      dMeas["fields"] = dict()
      dMeas["fields"]["agentname"] = host["name"]
      dMeas["tags"]["agentname"] = host["name"]
      dMeas["tags"]["host"] = host["host"]
      dMeas["tags"]["component"] = item
      for key in result[item].keys():
        if key != "Type":
          dMeas["fields"][key]=eval(result[item][key])
        else:
          dMeas["measurement"]='flume_'+result[item][key]
      
      line_prot = str(dMeas["measurement"])
      for tag_key in dMeas["tags"].keys():
        line_prot+=","+tag_key+"="+dMeas["tags"][tag_key]
      line_prot+=" "
      first = 0
      for m_key in dMeas["fields"].keys():
        if first == 0:
          first = 1
        else:
          line_prot+=','
        if type(dMeas["fields"][m_key]) == int:
          line_prot+=m_key+"="+str(dMeas["fields"][m_key])+"i"
        elif type(dMeas["fields"][m_key]) == float:
          line_prot+=m_key+"="+str(dMeas["fields"][m_key])
        elif type(dMeas["fields"][m_key]) == long:
          line_prot+=m_key+"="+str(dMeas["fields"][m_key])+"i"
        elif type(dMeas["fields"][m_key]) == bool:
          line_prot+=m_key+"="+str(dMeas["fields"][m_key])+""
        else:
          line_prot+=m_key+"=\""+dMeas["fields"][m_key]+"\""
      line_prot+=" "
      line_prot+=str(timestamp_ns)
      print line_prot
exit(0)
