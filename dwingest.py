#!/usr/bin/env python
"""
Allows to ingest data from dweet.io into InfluxDB.

Usage: 

  `python dwingest.py`  ... run with defaults

@author: Michael Hausenblas, http://mhausenblas.info/#i
@since: 2014-09-06
@status: init
"""

import sys
import os
import logging
import urllib2
import json
from influxdb import client as influxdb

################################################################################
# defaults
#

DEBUG = True

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


################################################################################
# main script
#
if __name__ == '__main__':
  host = HOST
  try:
      if sys.argv[1]:
        host = sys.argv[1]
      logging.info("Using InfluxDB host at %s:%s" %(host, PORT))
      ifx_client = influxdb.InfluxDBClient(host, PORT, USER_NAME, USER_PWD, DATABASE)
      push_datapoint(ifx_client, "AvocadoGrove", "aiHotWaterTemp_degreesF")
  except Exception, e:
    logging.error(e)
    sys.exit(2)
