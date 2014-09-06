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


def parse_value():
  return "thing_abc", "k0", "123"

def push_datapoint(ifx_client):
    dthing, dkey, dvalue = parse_value()
    datapoint = [{
            "name": DATABASE,
            "columns": ["thing", dkey],
            "points": [
                [dthing, dvalue]
            ]
        }]
    logging.debug("Ingesting {0}".format(datapoint))
    ifx_client.write_points(datapoint)


################################################################################
# main script
#
if __name__ == '__main__':
  host = HOST
  port = PORT
  
  
  try:
      if sys.argv[1]:
        host = sys.argv[1]
      if sys.argv[2]:
        port = sys.argv[2]
      ifx_client = influxdb.InfluxDBClient(host, port, USER_NAME, USER_PWD, DATABASE)
      push_datapoint(ifx_client)
  except Exception, e:
    logging.error(e)
    sys.exit(2)
    
