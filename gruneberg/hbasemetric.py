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

from gmetric import GmetricConfig, Gmetric

# Setup logging facilities
log = logging.getLogger("")
log.setLevel(logging.DEBUG)

'''
A class to emit Ganglia metrics based on values in HBase tables
'''
class HBaseMetric:
    def __init__(self, hbc, gmetric):
        self.hbc = hbc
        self.gmetric = gmetric
        
    def emit_row(self, table_name, row_id, gmetric_config):
        results = self.hbc.get_client().getRow(table_name, row_id)
        result_dict = {}
        for r in results:
            for column_name,v in r.columns.items():
                result_dict[column_name] = long(struct.unpack('>q', v.value)[0])

        for column_name,column_value in result_dict.iteritems():
            log.debug("%s => %s" % (column_name, column_value))
            metric_name = "%s.%s" % (table_name, column_name)
            self.gmetric.send_meta(metric_name, gmetric_config.type, gmetric_config.units, gmetric_config.slope, gmetric_config.tmax, gmetric_config.dmax, gmetric_config.group_name)
            self.gmetric.send(metric_name, column_value, gmetric_config.type)
                
'''
Wrapper class for an HBase thrift connection
'''
class HBaseConnection:
    def __init__(self, server_name, port):    
        # Make socket
        self.transport = TSocket.TSocket(server_name, port)
        
        # Buffering is critical. Raw sockets are very slow
        self.transport = TTransport.TBufferedTransport(self.transport)
        
        # Wrap in a protocol
        protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.client = Hbase.Client(protocol)
        log.debug("Opening HBase Connection")
        self.transport.open()

    def get_client(self):
        return self.client
        
    def close(self):
        log.debug("Closing HBase Connection")
        self.transport.close()
        
'''
Print usage information
'''
def usage():
    print "Usage: %s [--help] [--hbase_thrift server[:port]] [--gmetric server[:port]] table row_id" % (sys.argv[0])
    
def main(argv=None):
    # parse command line options
    try:
        opts, args = getopt.getopt(sys.argv[1:], "h", ["help", "hbase_thrift=", "gmetric="])
    except getopt.error, msg:
        print msg
        print "for help use --help"
        sys.exit(2)
    
    thrift_server = "localhost"
    thrift_server_port = 9090
    ganglia_host = "localhost"
    ganglia_port = 8649
    ganglia_protocol = "multicast"
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
                
    # process arguments
    row_id = None
    table_name = None
    if len(args) > 1:
        table_name = args[0]
        row_id = args[1]
    
    hbc = None
    try:
        hbc = HBaseConnection(thrift_server, thrift_server_port)
        gmetric = Gmetric(ganglia_host, ganglia_port, ganglia_protocol)
        hbm = HBaseMetric(hbc, gmetric)
        gmc = GmetricConfig(type="int32", group_name="socorro")
        hbm.emit_row(table_name, row_id, gmc)                
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