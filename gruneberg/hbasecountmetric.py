#!/usr/bin/env python
import sys
import getopt
import struct
import logging
import re
from datetime import datetime, timedelta

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from hbase import Hbase
from hbase.ttypes import *
from hbaseconnection import HBaseConnection

from gmetric import GmetricConfig, Gmetric

# Setup logging facilities
log = logging.getLogger("")
log.setLevel(logging.DEBUG)

'''
A class to emit Ganglia metrics based on the number of rows in HBase tables
'''
class HBaseCountMetric:
    def __init__(self, hbc, gmetric):
        self.hbc = hbc
        self.gmetric = gmetric
        
    def emit_row(self, table_name, gmetric_config):
        client = self.hbc.get_client()
        scanner_id = client.scannerOpen(table_name, "", [])
        try:
            count = 0
            while client.scannerGet(scanner_id):
                count += 1
            
            log.debug("%s => %s" % (table_name, count))
            metric_name = "%s" % (table_name)
            self.gmetric.send_meta(metric_name, gmetric_config.type, gmetric_config.units, gmetric_config.slope, gmetric_config.tmax, gmetric_config.dmax, gmetric_config.group_name)
            self.gmetric.send(metric_name, count, gmetric_config.type)
        finally:
            client.scannerClose(scanner_id)        
        
'''
Print usage information
'''
def usage():
    print "Usage: %s [--help] [--hbase_thrift server[:port]] [--gmetric server[:port]] table" % (sys.argv[0])
    
def main(argv=None):
    # parse command line options
    try:
        opts, args = getopt.getopt(sys.argv[1:], "h", ["help", "hbase_thrift=", "gmetric=", "group="])
    except getopt.error, msg:
        print msg
        print "for help use --help"
        sys.exit(2)
    
    thrift_server = "localhost"
    thrift_server_port = 9090
    ganglia_host = "localhost"
    ganglia_port = 8649
    ganglia_protocol = "multicast"
    ganglia_metic_group_name = "hbase"
    # process options
    for o, a in opts:
        if o in ("-h", "--help"):
            usage()
            sys.exit(0)
        elif o == "--hbase_thrift":
            splits = a.split(":")
            thrift_server = splits[0]
            if len(splits) == 2:
                thrift_server_port = splits[1]
        elif o == "--gmetric":
            splits = a.split(":")
            ganglia_host = splits[0]
            if len(splits) == 2:
                ganglia_port = splits[1]
        elif o == "--group":
            ganglia_metric_group_name = a
                
    # process arguments
    table_name = None
    if len(args) > 0:
        table_name = args[0]
    
    hbc = None
    try:
        hbc = HBaseConnection(thrift_server, thrift_server_port)
        gmetric = Gmetric(ganglia_host, ganglia_port, ganglia_protocol)
        hbm = HBaseCountMetric(hbc, gmetric)
        gmc = GmetricConfig(type="int32", group_name=ganglia_metic_group_name)
        hbm.emit_row(table_name, gmc)                
    except Thrift.TException, tx:
        logging.error("Thrift exception: %s" % (tx.message))
    finally:
        if hbc:
            hbc.close()
    
if __name__ == "__main__":
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    logging.getLogger('').addHandler(console)
    sys.exit(main())