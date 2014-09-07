#!/usr/bin/env python
"""
Allows to ingest data from dweet.io into InfluxDB. You must supply the InfluxDB
hostname, the thing (cf. https://dweet.io/see)) and the thing's key, that is,
a key in the JSON map representation of the thing. You can supply a poll interval,
and if you don't supply one 10 sec will be used as default.

Usage: 

   To run using host `influxdb` to ingest a data point of thing `AvocadoGrove` 
   with key `aiHotWaterTemp_degreesF` every 10 sec (that is, the default):
     
  `python dwingest.py influxdb AvocadoGrove aiHotWaterTemp_degreesF`

@author: Michael Hausenblas, http://mhausenblas.info/#i
@since: 2014-09-06
@status: init
"""

import sys
import time
import logging
import urllib2
import json
from influxdb import client as influxdb

################################################################################
# defaults
#

DEBUG = False

if DEBUG:
  FORMAT = '%(asctime)-0s %(levelname)s %(message)s [at line %(lineno)d]'
  logging.basicConfig(level=logging.DEBUG, format=FORMAT, datefmt='%Y-%m-%dT%I:%M:%S')
else:
  FORMAT = '%(asctime)-0s %(message)s'
  logging.basicConfig(level=logging.INFO, format=FORMAT, datefmt='%Y-%m-%dT%I:%M:%S')

HOST =      "localhost"
PORT =      8086
USER_NAME = "dweet"
USER_PWD =  "dweet"
DATABASE =  "dweet"

DWEETIO_BASE = "https://dweet.io:443/get/latest/dweet/for/"

DEFAULT_WAIT = 10 # that is, wait per default for 10 sec between trying to query
                  # dweet.io and push a new data point into InfluxDB

def parse_value(the_thing, the_key):
  try:
    response = urllib2.urlopen(DWEETIO_BASE + the_thing).read()
    logging.debug("{0}".format(response))
    data = json.loads(response) 
    dvalue = data["with"][0]["content"][the_key]
    return dvalue
  except Exception, e:
    logging.error(e)
    sys.exit(2)

def push_datapoint(ifx_client, the_thing, the_key):
  dvalue = parse_value(the_thing, the_key)
  datapoint = [{
          "name": DATABASE,
          "columns": ["thing", the_key],
          "points": [
              [the_thing, dvalue]
          ]
  }]
  logging.debug("Ingesting {0}".format(datapoint))
  ifx_client.write_points(datapoint)
  return dvalue

def ingest(host, thing, key, poll_wait):
  ifx_client = influxdb.InfluxDBClient(host, PORT, USER_NAME, USER_PWD, DATABASE)
  logging.info("="*80)
  logging.info("Querying thing %s with key %s every %d sec" %(thing, key, poll_wait))
  while True:
    logging.info("-"*80)
    dvalue = push_datapoint(ifx_client, thing, key)
    logging.info("Ingested value: %s" %(dvalue))
    time.sleep(poll_wait)

def usage():
  print("Usage:")
  print(" python dwingest.py $INFLUXDB_HOSTNAME $THING $KEY $POLL_INTERVAL")
  print("\nExample:")
  print(" python dwingest.py influxdb AvocadoGrove aiHotWaterTemp_degreesF")
  print(" python dwingest.py influxdb AvocadoGrove aiHotWaterTemp_degreesF 20")

################################################################################
# main script
#
if __name__ == '__main__':
  try:
      if len(sys.argv) < 4:          # we don't know the DB, thing and key ...
        usage()                      # ... show how to use it and ...
        sys.exit(2)                  # ... don't even think about trying
      elif len(sys.argv) == 4:       # we know all but the poll interval ...
        poll_wait = DEFAULT_WAIT     # ... use default poll interval
      else:                          #
        poll_wait = int(sys.argv[4]) # parse and use supplied poll interval

      host = sys.argv[1]
      thing = sys.argv[2]
      key = sys.argv[3]      
      logging.info("Using InfluxDB host at %s:%s" %(host, PORT))
      ingest(host, thing, key, poll_wait)
  except Exception, e:
    logging.error(e)
    usage()
    sys.exit(2)
